#!/usr/bin/env python3
"""
Tracker multi-sites - VERSION FINALE AVEC CAPTURE CONTINUE
─────────────────────────────────────────────────────────
Fonctionnalités :
├─ externalId au lieu de betRadarId (identifiant unique)
├─ GetSport avec pagination parallèle (tous les matchs du jour)
├─ Capture dynamique toutes les 2 minutes (matchs proches)
├─ CAPTURE CONTINUE pour matchs < 60 min (mise à jour cotes)
├─ DÉTECTION CHANGEMENTS de cotes (seuil > 0.01)
├─ SYSTÈME DE RETRY pour matchs sans cotes (max 5 tentatives)
├─ Capture IMMÉDIATE avant disparition (même 9h avant)
├─ Optimisation quotas Google Sheets (cache + retry)
├─ Resynchronisation Excel → Google Sheets
├─ append_rows() au lieu de batch_update() (fix désynchronisation)
├─ Gestion matchs sans cotes (évite boucle infinie)
├─ Surveillance multi-niveau (< 15 min, 15-60 min, > 60 min)
├─ File d'attente avec priorités
├─ Nom compétition depuis GetMatch
├─ Finalisation matchs déjà commencés
├─ 4 sites en parallèle
├─ Excel local + Google Sheets
├─ Fuseau horaire Maurice (UTC+4)
├─ PERSISTENCE DU CACHE (sauvegarde/restauration sur disque)
├─ ANTI DOUBLE-FINALISATION (dédup queue + verrou en cours)

Auteur: antema102
Date: 2025-11-18
Version: 2.5 (persistence + anti double finalisation + DailyCombinaison)
"""
import sys
import asyncio

# FIX WINDOWS
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path
from collections import defaultdict, deque
import traceback
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser as date_parser

# Configuration fuseau horaire Maurice
MAURITIUS_TZ = ZoneInfo("Indian/Mauritius")


def now_mauritius():
    return datetime.now(MAURITIUS_TZ)


def now_mauritius_iso():
    return datetime.now(MAURITIUS_TZ).isoformat()


def now_mauritius_str(fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now(MAURITIUS_TZ).strftime(fmt)


def now_mauritius_date_str():
    """Retourne la date actuelle à Maurice (format ISO pour API)"""
    return datetime.now(MAURITIUS_TZ).strftime("%Y-%m-%d")


# Configuration sites
SITES = {
    "stevenhills": {"name": "StevenHills", "base_url": "https://stevenhills.bet"},
    "superscore": {"name": "SuperScore", "base_url": "https://superscore.mu"},
    "totelepep": {"name": "ToteLePEP", "base_url": "https://www.totelepep.mu"},
    "playonlineltd": {"name": "PlayOnline", "base_url": "http://playonlineltd.net"}
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://stevenhills.bet/",
}

TRACKING_INTERVAL_SECONDS = 120
TIMEOUT = 20
MAX_RETRIES = 3

# Fenêtres temporelles
INTENSIVE_CAPTURE_WINDOW_MINUTES = 15
CLOSE_MATCH_WINDOW_MINUTES = 60
FINALIZATION_WINDOW_MINUTES = 10

DAILY_COMBINAISON_ENABLED = True
DAILY_COMBINAISON_MAX_TIME_BEFORE_MATCH = 360  # minutes (6h)

# Configuration Google Sheets
GOOGLE_SHEETS_CONFIG = {
    "credentials_file": "eternal-outlook-441811-k7-9e7b056d27b2.json",
    "sheet_name": "ALL_MATCHES",
}

# Mapping marchés
MARKET_SHEET_MAPPING = {
    "CP_FT": "1X2_FullTime",
    "CP_H1": "1X2_HalfTime",
    "CP_H2": "1X2_2ndHalf",
    "UO_FT_+1.5": "OverUnder_1.5_FT",
    "UO_FT_+2.5": "OverUnder_2.5_FT",
    "UO_FT_+3.5": "OverUnder_3.5_FT",
    "UO_H1_+1.5": "OverUnder_1.5_HT",
    "UO_H1_+2.5": "OverUnder_2.5_HT",
    "UO_H2_+1.5": "OverUnder_1.5_2H",
    "UO_H2_+2.5": "UO_H2_+2.5",
    "DC_FT": "DoubleChance_FT",
    "DC_H1": "DoubleChance_HT",
    "DC_H2": "DoubleChance_2H",
    "BT_FT": "BothTeamsToScore_FT",
    "BT_H1": "BothTeamsToScore_HT",
    "BT_H2": "BothTeamsToScore_2H",
    "CS_FT": "CorrectScore_FT",
    "CS_H1": "CorrectScore_HT",
    "CS_H2": "CorrectScore_2H",
    "OE_FT": "OddEven_FT",
    "OE_H1": "OE_H1",
    "OE_H2": "OE_H2",
    "HF_FT": "HalfTimeFullTime",
    "DB_FT": "DrawNoBet_FT",
    "GM_FT": "GoalMarket_FT",
    "GM_H1": "GoalMarket_HT",
    "GM_H2": "GoalMarket_2H",
    "WM_FT": "WinningMargin_FT",
    "FS_FT": "FirstToScore_FT",
    "LS_FT": "LastToScore_FT",
    "HSH_FT": "HighestScoringHalf_FT",
    "HSHA_FT": "HSHA_FT",
    "HSHH_FT": "HSHH_FT",
}


class MatchFinalizationQueue:
    """File d'attente pour les matchs à finaliser avec système de priorités"""

    def __init__(self, batch_size: int = 5, min_interval_seconds: int = 20):
        self.queue = deque()
        self.processing = False
        self.last_batch_time = None

        self.batch_size = batch_size
        self.min_interval_seconds = min_interval_seconds

        # Anti-doublon dans la file
        self.queued_ids: Set[int] = set()

        # Thread-safety lock for concurrent access
        self._lock = asyncio.Lock()

    async def add_match(self, external_id: int, priority: str = "normal"):
        """Ajouter un match à la queue (anti-doublon + upgrade priorité)"""
        async with self._lock:
            # Si déjà en file, upgrade éventuelle de priorité
            if external_id in self.queued_ids:
                for item in self.queue:
                    if item["external_id"] == external_id:
                        if priority == "urgent" and item["priority"] == "normal":
                            item["priority"] = "urgent"
                return

            self.queue.append({
                "external_id": external_id,
                "priority": priority,
                "added_at": now_mauritius()
            })
            self.queued_ids.add(external_id)

    async def get_next_batch(self) -> List[int]:
        """Récupérer le prochain batch de matchs"""
        async with self._lock:
            if not self.queue:
                return []

            if self.last_batch_time:
                elapsed = (now_mauritius() -
                           self.last_batch_time).total_seconds()
                if elapsed < self.min_interval_seconds:
                    return []

            urgent = [item for item in self.queue if item["priority"] == "urgent"]
            normal = [item for item in self.queue if item["priority"] == "normal"]

            batch_items = (urgent + normal)[:self.batch_size]
            batch_ids = [item["external_id"] for item in batch_items]

            for item in batch_items:
                self.queue.remove(item)
                self.queued_ids.discard(item["external_id"])

            self.last_batch_time = now_mauritius()

            return batch_ids

    def __contains__(self, external_id: int) -> bool:
        return external_id in self.queued_ids

    def __len__(self):
        return len(self.queue)


class APIHealthMonitor:
    """Moniteur de santé des API GetSport et GetMatch"""

    def __init__(self):
        self.stats = {
            "getsport": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []}),
            "getmatch": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []})
        }
        self.last_reset = now_mauritius()

    def record_getsport(self, site_key: str, success: bool, error_msg: str = ""):
        """Enregistrer un appel GetSport"""
        if success:
            self.stats["getsport"][site_key]["success"] += 1
        else:
            self.stats["getsport"][site_key]["failed"] += 1
            if error_msg:
                self.stats["getsport"][site_key]["errors"].append({
                    "time": now_mauritius_str("%H:%M:%S"),
                    "error": error_msg
                })

    def record_getmatch(self, site_key: str, success: bool, error_msg: str = ""):
        """Enregistrer un appel GetMatch"""
        if success:
            self.stats["getmatch"][site_key]["success"] += 1
        else:
            self.stats["getmatch"][site_key]["failed"] += 1
            if error_msg:
                # Limiter à 5 dernières erreurs
                if len(self.stats["getmatch"][site_key]["errors"]) >= 5:
                    self.stats["getmatch"][site_key]["errors"].pop(0)
                self.stats["getmatch"][site_key]["errors"].append({
                    "time": now_mauritius_str("%H:%M:%S"),
                    "error": error_msg
                })

    def get_health_score(self, site_key: str) -> float:
        """Calculer score de santé (0-100)"""
        total_success = (
            self.stats["getsport"][site_key]["success"] +
            self.stats["getmatch"][site_key]["success"]
        )
        total_failed = (
            self.stats["getsport"][site_key]["failed"] +
            self.stats["getmatch"][site_key]["failed"]
        )

        total = total_success + total_failed
        if total == 0:
            return 100.0

        return (total_success / total) * 100

    def print_report(self):
        """Afficher rapport de santé"""
        print(f"\n   🏥 Santé API (dernière heure) :")

        for site_key in SITES.keys():
            score = self.get_health_score(site_key)

            if score >= 95:
                emoji = "🟢"
            elif score >= 80:
                emoji = "🟡"
            elif score >= 50:
                emoji = "🟠"
            else:
                emoji = "🔴"

            gs_success = self.stats["getsport"][site_key]["success"]
            gs_failed = self.stats["getsport"][site_key]["failed"]
            gm_success = self.stats["getmatch"][site_key]["success"]
            gm_failed = self.stats["getmatch"][site_key]["failed"]

            gs_total = gs_success + gs_failed
            gm_total = gm_success + gm_failed

            print(
                f"      {emoji} {SITES[site_key]['name']:15s}: {score:5.1f}% ", end="")

            if gs_total > 0 or gm_total > 0:
                print(
                    f"(GetSport: {gs_success}/{gs_total}, GetMatch: {gm_success}/{gm_total})")
            else:
                print("(Aucune tentative)")

            # Afficher dernières erreurs si score faible
            if score < 90 and (gs_failed > 0 or gm_failed > 0):
                recent_errors = (
                    self.stats["getsport"][site_key]["errors"][-2:] +
                    self.stats["getmatch"][site_key]["errors"][-2:]
                )
                for err in recent_errors:
                    print(f"         └─ {err['time']}: {err['error']}")

    def reset_if_needed(self):
        """Reset toutes les heures"""
        if (now_mauritius() - self.last_reset).total_seconds() > 3600:
            print(f"\n   🔄 Statistiques API réinitialisées (nouvelle heure)")
            self.stats = {
                "getsport": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []}),
                "getmatch": defaultdict(lambda: {"success": 0, "failed": 0, "errors": []})
            }
            self.last_reset = now_mauritius()


class GoogleSheetsManager:
    """Gestionnaire Google Sheets OPTIMISÉ avec append au lieu de batch_update"""
    API_QUOTA_HIGH_THRESHOLD = 40
    API_QUOTA_CRITICAL_THRESHOLD = 50

    def __init__(self, credentials_file: str, sheet_name: str):
        self.credentials_file = credentials_file
        self.sheet_name = sheet_name
        self.client = None
        self.spreadsheet = None
        self.worksheets = {}
        self.headers_initialized = set()

        self.last_row_cache = {}

        self.last_api_call = None
        self.min_interval_between_calls = 1.0

        self.api_call_count = 0
        self.api_call_window_start = time.time()
        self._last_summary_update = 0

    def connect(self):
        """Se connecter à Google Sheets"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file, scope
            )

            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(self.sheet_name)

            print(f"✅ Connecté à Google Sheets: {self.sheet_name}")
            return True

        except FileNotFoundError:
            print(f"❌ Fichier credentials non trouvé: {self.credentials_file}")
            return False

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"❌ Google Sheet non trouvé: {self.sheet_name}")
            return False

        except Exception as e:
            print(f"❌ Erreur connexion Google Sheets: {e}")
            return False

    def _track_api_call(self):
        """Tracker avec affichage en temps réel"""
        now = time.time()

        if now - self.api_call_window_start > 60:
            if self.api_call_count > 0:
                print(
                    f"   📊 Dernière minute : {self.api_call_count}/60 appels")
            self.api_call_count = 0
            self.api_call_window_start = now

        self.api_call_count += 1

        # AFFICHER TOUS LES 10 APPELS
        if self.api_call_count % 10 == 0:
            print(f"         📊 {self.api_call_count}/60 appels API...")

        # RALENTIR AUTOMATIQUEMENT
        if self.api_call_count >= 45:
            print(
                f"         ⚠️  Quota élevé ({self.api_call_count}/60) → ralentissement")
            self.min_interval_between_calls = 3.0
        elif self.api_call_count >= 50:
            print(
                f"         🚨 QUOTA CRITIQUE ({self.api_call_count}/60) → pause forcée")
            time.sleep(5)

    def _wait_if_needed(self):
        """Attendre si nécessaire pour respecter le rate limit"""
        self._track_api_call()

        if self.last_api_call is None:
            self.last_api_call = time.time()
            return

        elapsed = time.time() - self.last_api_call

        if elapsed < self.min_interval_between_calls:
            wait_time = self.min_interval_between_calls - elapsed
            time.sleep(wait_time)

        self.last_api_call = time.time()

    def _execute_with_retry(self, func, max_retries=2, backoff_factor=3):
        """Exécuter avec retry réduit"""
        for attempt in range(max_retries):
            try:
                self._wait_if_needed()

                # TIMEOUT via gspread
                import socket
                original_timeout = socket.getdefaulttimeout()
                socket.setdefaulttimeout(60)  # ✅ ENHANCEMENT #6: Increased from 30s to 60s for slow connections

                try:
                    result = func()
                    return result
                finally:
                    socket.setdefaulttimeout(original_timeout)

            except (gspread.exceptions.APIError, socket.timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** (attempt + 1)
                    print(f"         ⏸️  Erreur API → Attente {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"         ❌ Échec après {max_retries} tentatives")
                    raise

    def get_or_create_worksheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """Obtenir ou créer une feuille"""
        try:
            if sheet_name in self.worksheets:
                return self.worksheets[sheet_name]

            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=20
                )
                print(f"   📄 Nouvelle feuille créée: {sheet_name}")

            self.worksheets[sheet_name] = worksheet
            return worksheet

        except Exception as e:
            print(f"   ⚠️  Erreur feuille {sheet_name}: {e}")
            return None

    def _ensure_header(self, worksheet: gspread.Worksheet, sheet_name: str, expected_header: List[str]) -> bool:
        """Garantir que le header est correct"""
        try:
            if sheet_name in self.headers_initialized:
                return True

            try:
                first_row = worksheet.row_values(1)
            except:
                first_row = []

            if not first_row or all(not cell for cell in first_row):
                def update_header():
                    return worksheet.update(values=[expected_header], range_name='A1')
                self._execute_with_retry(update_header)
                self.headers_initialized.add(sheet_name)
                return True

            if first_row == expected_header:
                self.headers_initialized.add(sheet_name)
                return True

            self.headers_initialized.add(sheet_name)
            return True

        except Exception as e:
            return False

    def invalidate_cache(self):
        """Invalider le cache"""
        if self.last_row_cache:
            print(
                f"   🧹 Cache Google Sheets invalidé ({len(self.last_row_cache)} feuilles)")
        self.last_row_cache.clear()

    async def append_rows_batch(self, sheets_data: Dict[str, List[Dict]]):
        """Utiliser append() avec pauses adaptatives"""
        try:
            print(
                f"\n      📤 Préparation batch ({len(sheets_data)} feuilles)...")

            total_rows_sent = 0
            feuilles_traitees = 0

            for sheet_name, rows in sheets_data.items():
                if not rows:
                    continue

                worksheet = self.get_or_create_worksheet(sheet_name)
                if not worksheet:
                    continue

                expected_header = list(rows[0].keys())

                # ✅ AJOUTER : Validation avant écriture
                for i, row in enumerate(rows):
                    if list(row.keys()) != expected_header:
                        print(f"         ⚠️  Ligne {i+1} décalage de colonnes!")
                        print(f"            Attendu: {expected_header}")
                        print(f"            Obtenu: {list(row.keys())}")
                        
                # Afficher les différences
                self._ensure_header(worksheet, sheet_name, expected_header)

                data_to_append = [list(row.values()) for row in rows]

                print(
                    f"         📋 '{sheet_name}' → {len(data_to_append)} ligne(s)")

                try:
                    def append_data():
                        return worksheet.append_rows(
                            data_to_append,
                            value_input_option='RAW',
                            insert_data_option='INSERT_ROWS',
                            table_range=None
                        )

                    # RETRY RÉDUIT + TIMEOUT
                    self._execute_with_retry(append_data, max_retries=2)
                    total_rows_sent += len(data_to_append)
                    feuilles_traitees += 1

                    # PAUSE ADAPTATIVE - only if quota is getting high
                    if feuilles_traitees % 5 == 0:
                        if self.api_call_count >= self.API_QUOTA_HIGH_THRESHOLD:
                            print(
                                f"         ⏸️  Pause quota ({feuilles_traitees}/32 feuilles)...")
                            await asyncio.sleep(15)
                        else:
                            await asyncio.sleep(2)  # Short pause
                    else:
                        await asyncio.sleep(2)  # 2s between each

                except Exception as e:
                    print(f"         ❌ '{sheet_name}': {e}")
                    await asyncio.sleep(5)
                    continue

            if total_rows_sent > 0:
                print(
                    f"      ✅ {total_rows_sent} lignes envoyées ({feuilles_traitees} feuilles)")
                return True
            else:
                print(f"      ⚠️  Aucune donnée envoyée")
                return False

        except Exception as e:
            print(f"   ❌ Erreur envoi batch: {e}")
            traceback.print_exc()
            return False

    def update_summary(self, force=False):
        """Mettre à jour summary (avec throttle)"""

        now = time.time()

        if not force:
            if now - self._last_summary_update < 300:
                return

        try:
            worksheet = self.get_or_create_worksheet("Summary")

            summary_data = [
                ["ALL MATCHES CUMULATIVE - LIVE"],
                ["Dernière mise à jour", now_mauritius_str()],
                ["Fuseau horaire", "Indian/Mauritius (UTC+4)"],
                ["Utilisateur", "antema102"],
                ["Stratégie", "Capture dynamique + externalId + append() + fix boucle"],
                ["Sites", "StevenHills, SuperScore, ToteLePEP, PlayOnline"],
                [""],
                ["Feuilles disponibles:"],
            ]

            for sheet in self.spreadsheet.worksheets():
                if sheet.title != "Summary":
                    summary_data.append([f"  - {sheet.title}"])

            def update():
                worksheet.clear()
                return worksheet.update(values=summary_data, range_name='A1')

            self._execute_with_retry(update)

            self._last_summary_update = now

        except Exception as e:
            print(f"   ⚠️ Erreur update summary: {e}")


class MultiSitesOddsTrackerFinal:
    """Tracker FINAL avec externalId + Capture dynamique + append + fix boucle infinie"""

    # Iteration intervals
    FULL_GETSPORT_INTERVAL = 5
    DAILY_COMBO_UPDATE_INTERVAL = 3
    RETRY_WITHOUT_ODDS_INTERVAL = 2
    HEALTH_REPORT_INTERVAL = 5
    CLEANUP_INTERVAL = 10

    # API and error handling
    API_QUOTA_HIGH_THRESHOLD = 40
    ERROR_MESSAGE_MAX_LENGTH = 100

    # Limits
    MAX_MATCHING_MATCHES = 100  # Prevent sheet overload

    # ✅ AJOUTER CECI : Ordre fixe des sites pour un alignement cohérent des colonnes
    SITE_ORDER = ["stevenhills", "superscore", "totelepep", "playonlineltd"]

    def __init__(self, output_dir="multi_sites_odds"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.match_results_cache = {}

        (self.output_dir / "comparison").mkdir(exist_ok=True)

        self.session: Optional[aiohttp.ClientSession] = None

        self.matches_by_external_id: Dict[int,
                                          Dict[str, dict]] = defaultdict(dict)
        self.captured_odds: Dict[int, Dict[str, dict]] = defaultdict(dict)
        self.closed_sites: Dict[int, Set[str]] = defaultdict(set)
        self.completed_matches: Set[int] = set()
        self.matches_info_archive: Dict[int, Dict[str, dict]] = {}

        self.matches_snapshot: Dict[int, Dict[str, bool]] = {}
        self.early_closed: Dict[int, Set[str]] = defaultdict(set)
        self.current_date = now_mauritius_date_str()
        self.last_getsport_full = None

        # ✅ NOUVEAU : Timestamp vérification résultats
        self.last_result_check = None

        self.finalization_queue = MatchFinalizationQueue(
            batch_size=20,
            min_interval_seconds=10
        )

        self.iteration = 0
        self.start_time = now_mauritius()

        self.gsheets = GoogleSheetsManager(
            GOOGLE_SHEETS_CONFIG["credentials_file"],
            GOOGLE_SHEETS_CONFIG["sheet_name"]
        )

        self.local_cumulative_excel = self.output_dir / "ALL_MATCHES_CUMULATIVE_LIVE.xlsx"

        self.api_health = APIHealthMonitor()

        # Système de retry pour matchs sans cotes
        self.matches_without_odds_retry: Dict[int, Dict[str, any]] = {}
        self.max_retry_attempts = 5  # Nombre max de tentatives

        # Persistence état/cache
        self.state_file = self.output_dir / "tracker_state.pkl"
        self._last_cache_save = 0.0
        self. cache_save_min_interval = 30  # secondes (anti-spam disque)
        self._disable_auto_save = False

        # Anti double-finalisation
        self.finalizing_in_progress: Set[int] = set()

        # Cache DailyCombinaison
        self.daily_combinaison_cache = {}  # {external_id: {site: "1.20/3.00/4. 00"}}
        self.combinations_index_cache = {site_key: {}
                                         for site_key in SITES.keys()}
        self.combinations_index_cache["last_rebuild"] = None
        self.daily_combo_total_matches = 0
        self.daily_combo_matches_with_combos = 0
        self.daily_combo_total_combinations = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()

        if not self.gsheets.connect():
            raise Exception("Impossible de se connecter à Google Sheets")

        # Charger l'état persistant (si présent)
        self.load_cache_from_disk()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sauvegarder à la fermeture
        self.save_cache_to_disk(force=True)
        if self.session:
            await self.session.close()

    # ----- Persistence helpers -----
    def save_cache_to_disk(self, force: bool = False):
        """Sauvegarde l'état RAM sur disque (JSON) avec anti-spam."""
        try:
            if self._disable_auto_save and not force:
                return

            now = time.time()
            if not force and (now - self._last_cache_save) < self.cache_save_min_interval:
                return
            # Convert sets to lists for JSON serialization
            data = {
                "captured_odds": dict(self.captured_odds),
                "matches_info_archive": self.matches_info_archive,
                "matches_by_external_id": dict(self.matches_by_external_id),
                "closed_sites": {k: list(v) for k, v in self.closed_sites.items()},
                "completed_matches": list(self.completed_matches),
                "early_closed": {k: list(v) for k, v in self.early_closed.items()},
                "matches_snapshot": self.matches_snapshot,
                "matches_without_odds_retry": self.matches_without_odds_retry,
                "current_date": self.current_date,
                "iteration": self.iteration,
                "finalizing_in_progress": list(self.finalizing_in_progress),
                "saved_at": now_mauritius_str(),
            }
            json_file = self.state_file.with_suffix('.json')
            with open(json_file, 'w') as f:
                # default=str for datetime
                json.dump(data, f, indent=2, default=str)
            self._last_cache_save = now
            print(f"💾 État sauvegardé ({json_file.name})")
        except Exception as e:
            print(f"❌ Erreur sauvegarde cache : {e}")

    def _to_defaultdict(self, factory, d: Optional[dict]):
        """Transformer un dict en defaultdict avec factory pour compatibilité."""
        return defaultdict(factory, d or {})

    def load_cache_from_disk(self):
        """Charge l'état disque en RAM (JSON) et restaure les defaultdict."""
        try:
            json_file = self.state_file.with_suffix('.json')
            if not json_file.exists():
                print("ℹ️ Aucun cache sur disque à charger.")
                return
            with open(json_file, 'r') as f:
                data = json.load(f)

            # Restaurer en conservant les types attendus
            self.captured_odds = self._to_defaultdict(
                dict, data.get("captured_odds"))
            self.matches_by_external_id = self._to_defaultdict(
                dict, data.get("matches_by_external_id"))
            # closed_sites et early_closed: sets par valeur
            loaded_closed = data.get("closed_sites") or {}
            loaded_early = data.get("early_closed") or {}
            try:
                self.closed_sites = defaultdict(
                    set, {int(k): set(v) for k, v in loaded_closed.items()})
                self.early_closed = defaultdict(
                    set, {int(k): set(v) for k, v in loaded_early.items()})
            except (ValueError, TypeError) as e:
                print(
                    f"⚠️ Erreur conversion clés closed_sites/early_closed: {e}")
                self.closed_sites = defaultdict(set)
                self.early_closed = defaultdict(set)

            self.matches_info_archive = data.get("matches_info_archive") or {}
            self.completed_matches = set(data.get("completed_matches") or [])
            self.matches_snapshot = data.get("matches_snapshot") or {}
            self.matches_without_odds_retry = data.get(
                "matches_without_odds_retry") or {}
            self.current_date = data.get("current_date", self.current_date)
            self.iteration = int(data.get("iteration", self.iteration))
            self.finalizing_in_progress = set(
                data.get("finalizing_in_progress") or [])

            # Log résumé
            total_sites_cached = sum(len(s)
                                     for s in self.captured_odds.values())
            print(
                f"✅ Cache rechargé ({self.state_file.name}) — cotes en cache: {total_sites_cached} sites, matchs suivis: {len(self.matches_info_archive)}")
        except Exception as e:
            print(f"❌ Erreur chargement cache : {e}")

    # ----- Utils & odds handling -----
    def safe_float(self, value, default=0.0) -> float:
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                cleaned = value.strip().replace(',', '.')
                return float(cleaned) if cleaned else default
            return default
        except:
            return default

    def _get_time_until_match(self, start_time_str: str) -> Optional[float]:
        """Retourner le temps restant avant le match (en minutes)"""
        try:
            match_time_str = start_time_str.replace(',', '').strip()
            current_year = now_mauritius().year
            if str(current_year) not in match_time_str:
                match_time_str = f"{match_time_str} {current_year}"

            match_time = date_parser.parse(match_time_str)
            if match_time.tzinfo is None:
                match_time = match_time.replace(tzinfo=MAURITIUS_TZ)

            now = now_mauritius()
            time_diff_minutes = (match_time - now).total_seconds() / 60
            return time_diff_minutes
        except:
            return None

    def _is_match_close_to_start(self, start_time_str: str, minutes_before: int = 10) -> bool:
        """Vérifier si le match commence bientôt"""
        try:
            time_diff_minutes = self._get_time_until_match(start_time_str)

            if time_diff_minutes is None:
                return True

            return time_diff_minutes <= minutes_before

        except Exception as e:
            return True

    def _cleanup_old_matches(self):
        """Remove matches that finished more than 5 hours ago to prevent memory leak"""
        # Threshold for cleanup (in minutes)
        CLEANUP_OLD_MATCHES_THRESHOLD_MINUTES = 300

        now = now_mauritius()
        to_remove = []

        for eid in list(self.matches_info_archive.keys()):
            match_info = self.matches_info_archive.get(eid)
            if not match_info:
                continue

            first_match = list(match_info.values())[0]
            start_time = first_match.get("start_time", "")
            time_diff = self._get_time_until_match(start_time)

            # Remove if match finished > 5 hours ago (time_diff < -300 minutes)
            if time_diff is not None and time_diff < -CLEANUP_OLD_MATCHES_THRESHOLD_MINUTES:
                to_remove.append(eid)

        for eid in to_remove:
            if eid in self.matches_info_archive:
                del self.matches_info_archive[eid]
            if eid in self.captured_odds:
                del self.captured_odds[eid]
            if eid in self.closed_sites:
                del self.closed_sites[eid]

        if to_remove:
            print(f"   🧹 Cleaned up {len(to_remove)} old matches from memory")

    def _odds_have_changed(self, old_odds: dict, new_odds: dict) -> bool:
        """Détecter si les cotes ont changé"""
        try:
            old_markets = old_odds.get("markets", {})
            new_markets = new_odds.get("markets", {})

            # Vérifier même nombre de marchés
            if len(old_markets) != len(new_markets):
                return True

            # Comparer chaque marché
            for market_key in old_markets.keys():
                if market_key not in new_markets:
                    return True

                old_market_odds = old_markets[market_key].get("odds", {})
                new_market_odds = new_markets[market_key].get("odds", {})

                # Comparer les valeurs
                for option_key in old_market_odds.keys():
                    if option_key not in new_market_odds:
                        return True

                    old_value = old_market_odds[option_key].get("odd", 0)
                    new_value = new_market_odds[option_key].get("odd", 0)

                    # Différence > 0.01
                    if abs(old_value - new_value) > 0.01:
                        return True

            return False
        except:
            return True  # En cas d'erreur, considérer comme changé

    async def fetch(self, url: str, params: dict = None, site_name: str = "") -> Optional[dict]:
        """Fetch avec détection d'erreurs détaillée"""
        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    # Erreurs récupérables
                    if resp.status in (429, 500, 502, 503, 504):
                        last_error = f"HTTP {resp.status}"
                        if attempt < MAX_RETRIES:
                            wait = min(2 ** attempt, 5)
                            await asyncio.sleep(wait)
                            continue
                        else:
                            # Échec après toutes les tentatives
                            return None
                    else:
                        # Erreur non récupérable (404, 403, etc.)
                        last_error = f"HTTP {resp.status}"
                        return None

            except asyncio.TimeoutError:
                last_error = f"Timeout ({TIMEOUT}s)"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None

            except aiohttp.ClientError as e:
                last_error = f"ClientError: {type(e).__name__}"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)[:self.ERROR_MESSAGE_MAX_LENGTH]}"
                last_error = error_msg
                if site_name:
                    print(f"      ⚠️ Fetch error ({site_name}): {error_msg}")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 5))
                    continue
                return None

        # Si on arrive ici, toutes les tentatives ont échoué
        return None

    async def _build_results_cache_from_sheet(self) -> Dict[str, Dict[str, str]]:
        """
        Construire un cache des résultats depuis la feuille 1X2_FullTime
        
        Returns:
            {
                "stevenhills": {
                    "Arsenal vs Chelsea": "1-1",
                    "Manchester United vs Liverpool": "2-0",
                    "Real Madrid vs Barcelona": "3-1"
                },
                "superscore": {...},
                "totelepep": {...},
                "playonlineltd": {...}
            }
        """
        results_cache = {site_key: {} for site_key in self.SITE_ORDER}
        
        try:
            worksheet = self.gsheets.get_or_create_worksheet("1X2_FullTime")
            if not worksheet:
                return results_cache
            
            def get_all():
                return worksheet.get_all_values()
            
            all_values = self.gsheets._execute_with_retry(get_all)
            
            if not all_values or len(all_values) < 2:
                return results_cache
            
            header = all_values[0]
            
            # Trouver colonne Match
            try:
                col_match = header.index("Match")
            except ValueError:
                print("   ⚠️  Colonne Match manquante")
                return results_cache
            
            # Trouver colonne résultat
            try:
                col_result = header.index("Résultat_FullTime")
            except ValueError:
                print("   ⚠️  Colonne Résultat_FullTime manquante")
                return results_cache
            
            # Parser les résultats - LOAD ALL ROWS (no date filtering)
            for row in all_values[1:]:
                if len(row) <= max(col_match, col_result):
                    continue
                
                match_name = row[col_match]
                result = row[col_result]
                
                # Seulement si résultat valide
                if not match_name or not result or result == "C":
                    continue
                
                # Store result for all sites (duplicates will be overwritten with latest)
                for site_key in self.SITE_ORDER:
                    results_cache[site_key][match_name] = result
            
            total_results = sum(len(site_cache) for site_cache in results_cache.values())
            print(f"   📊 Cache résultats chargé : {total_results} résultats historiques")
            return results_cache
            
        except Exception as e:
            print(f"   ❌ Erreur cache résultats : {e}")
            return results_cache

    async def _update_separate_sheets_incremental(self, changed_matches: dict, combinations_index: dict):
        """Mettre à jour les 4 feuilles (format 1 colonne par match) - VERSION CORRIGÉE"""

        try:
            # ✅ NOUVEAU : Charger cache depuis Google Sheets
            print("   📊 Chargement résultats depuis 1X2_FullTime...")
            results_cache = await self._build_results_cache_from_sheet()
            
            # Récupérer noms des matchs modifiés
            changed_match_names = set()
            for external_id in changed_matches.keys():
                match_info = self.matches_info_archive.get(external_id, {})
                if match_info:
                    first_info = list(match_info.values())[0]
                    changed_match_names.add(first_info. get("match_name", ""))

            print(
                f"   🔄 {len(changed_match_names)} match(s) modifié(s) à mettre à jour")

            # Pour chaque site
            for site_key in SITES. keys():
                site_name = SITES[site_key]["name"]
                sheet_name = f"DailyCombinaison_{site_name}"

                print(f"   📝 {sheet_name}...")

                worksheet = self.gsheets. get_or_create_worksheet(sheet_name)
                if not worksheet:
                    continue

                # Récupérer données actuelles
                def get_all():
                    return worksheet.get_all_values()

                all_values = self. gsheets._execute_with_retry(get_all)

                if not all_values or len(all_values) < 2:
                    print(f"      ⚠️  Feuille vide → ignoré")
                    continue

                header = all_values[0]

                try:
                    col_match = header.index("Match Principal")
                except ValueError:
                    col_match = 1

                # ✅ NOUVEAU : Créer un dictionnaire des lignes existantes
                existing_rows = {}
                for row in all_values[1:]:
                    if len(row) > col_match:
                        match_name = row[col_match]
                        existing_rows[match_name] = row

                print(f"      📊 {len(existing_rows)} ligne(s) existante(s)")

                # ✅ NOUVEAU : Mettre à jour SEULEMENT les matchs modifiés
                updated_count = 0
                
                # ✅ BUG FIX #1: Calculate max number of similar matches across ALL changed matches
                max_matches_per_row = 0
                for external_id in changed_matches.keys():
                    match_info = self.matches_info_archive.get(external_id, {})
                    if not match_info:
                        continue
                    sites_odds = self.daily_combinaison_cache.get(external_id, {})
                    if site_key not in sites_odds:
                        continue
                    
                    odds_string = sites_odds[site_key]
                    odds_key = odds_string.replace(" ", "")
                    all_matches = combinations_index[site_key].get(odds_key, [])
                    matching_matches = [m for m in all_matches if m.get("external_id") != external_id]
                    
                    if len(matching_matches) > max_matches_per_row:
                        max_matches_per_row = len(matching_matches)
                
                # ✅ BUG FIX #1: Rebuild dynamic header
                base_header = ["Date", "Match Principal", "Compétition", "Heure Match", "Cotes 1X2"]
                for i in range(1, max_matches_per_row + 1):
                    base_header.append(f"Match {i}")
                    base_header.append(f"Heure {i}")
                base_header.append("Nombre Total")
                
                header = base_header  # Use dynamic header
                
                # ✅ BUG FIX #1: Pad existing rows with empty strings if they have fewer columns
                for match_name, row in existing_rows.items():
                    if len(row) < len(header):
                        row.extend([""] * (len(header) - len(row)))
                    existing_rows[match_name] = row

                for external_id in changed_matches.keys():
                    match_info = self.matches_info_archive.get(external_id, {})
                    if not match_info:
                        continue

                    sites_odds = self.daily_combinaison_cache.get(
                        external_id, {})

                    if site_key not in sites_odds:
                        continue

                    first_info = list(match_info.values())[0]
                    match_principal = first_info.get("match_name", "")
                    competition_principal = first_info.get(
                        "competition_name", "")
                    heure_principal = first_info.get("start_time", "")

                    odds_string = sites_odds[site_key]
                    odds_key = odds_string.replace(" ", "")

                    all_matches = combinations_index[site_key].get(
                        odds_key, [])
                    matching_matches = [
                        m for m in all_matches if m.get("external_id") != external_id
                    ]

                    # Trier et limiter
                    matching_matches_sorted = sorted(
                        matching_matches,
                        key=lambda m: self._get_time_until_match(
                            m["start_time"]) or 9999
                    )[:self.MAX_MATCHING_MATCHES] if matching_matches else []

                    # Créer la nouvelle ligne
                    new_row = {
                        "Date": now_mauritius_str("%Y-%m-%d"),
                        "Match Principal": match_principal,
                        "Compétition": competition_principal,
                        "Heure Match": heure_principal,
                        "Cotes 1X2": odds_string,
                    }

                    # Ajouter matchs similaires avec résultats
                    for i, match in enumerate(matching_matches_sorted, start=1):
                        match_name = match.get('match_name', '')
                        competition = match.get('competition', '')
                        
                        # ✅ Direct lookup by match name (no external_id or match_id needed)
                        match_result = ""
                        if match_name in results_cache.get(site_key, {}):
                            ft = results_cache[site_key][match_name]
                            match_result = f" [{ft}]"

                        new_row[f"Match {i}"] = f"{match_name} ({competition}){match_result}"
                        new_row[f"Heure {i}"] = match.get('start_time', '')

                    new_row["Nombre Total"] = len(matching_matches_sorted)

                    # ✅ Remplacer ou ajouter la ligne dans le dictionnaire
                    existing_rows[match_principal] = [
                        new_row.get(col, "") for col in header]
                    updated_count += 1

                # ✅ NOUVEAU : Réécrire TOUTE la feuille avec lignes existantes + mises à jour
                if updated_count > 0:
                    # Convertir dictionnaire en liste
                    all_data = [header]
                    for row_values in existing_rows.values():
                        all_data.append(row_values)

                    # Effacer et réécrire
                    def clear_sheet():
                        return worksheet.clear()

                    self.gsheets._execute_with_retry(clear_sheet)
                    await asyncio.sleep(2)

                    # Réécrire par batch
                    batch_size = 500
                    for i in range(0, len(all_data), batch_size):
                        batch = all_data[i:i+batch_size]

                        def write_batch():
                            return worksheet.update(values=batch, range_name=f"A{i+1}")

                        self.gsheets._execute_with_retry(write_batch)

                        if i + batch_size < len(all_data):
                            await asyncio.sleep(3)

                    print(
                        f"      ✅ Mis à jour ({len(all_data)-1} lignes, {updated_count} modifiée(s))")
                else:
                    print(f"      ➡️  Aucun changement pour ce site")

            print(f"   ✅ 4 feuilles mises à jour")

        except Exception as e:
            print(f"      ❌ Erreur : {e}")
            traceback.print_exc()

    async def poll_all_sites_and_detect_odds_changes(self):
        """
        Récupère toutes les pages pour chaque site, détecte les changements de cotes 1X2,
        et met à jour les feuilles DailyCombinaison uniquement pour les matchs/sites modifiés.
        Extraction FIABLE basée sur match_str brut.
        """
        print("\n=== POLLING GetSport des 4 sites & détection des changements de cotes ===")
        date = self.current_date

        # 1. Récupérer tous les matchs du jour pour chaque site (pagination parallèle)
        all_matches_by_site = await self.get_all_sites_matches(date)

        changes_by_match = {}  # external_id: {site_key: {...}}

        # 2. Pour chaque match/site: extraire la cote 1X2 du raw_match_str
        for site_key, matches in all_matches_by_site.items():
            for external_id, match_info in matches.items():
                match_str = match_info.get("raw_match_str")
                if not match_str:
                    continue
                odds_1x2 = self._extract_1x2_from_getsport_match(match_str)
                if not odds_1x2:
                    continue
                
                # ✅ BUG FIX #3: Capture old value FIRST (before updating cache)
                old_odds = self.daily_combinaison_cache.get(
                    external_id, {}).get(site_key)
                
                # ✅ BUG FIX #3: Check for changes BEFORE updating cache
                if old_odds is not None and old_odds != odds_1x2:
                    # LOG le changement !
                    print(f"🔄 CHANGEMENT {external_id} ({SITES[site_key]['name']}) : "
                          f"{match_info.get('match_name', '')[:60]}\n    {old_odds}  ->  {odds_1x2}")
                    if external_id not in changes_by_match:
                        changes_by_match[external_id] = {}
                    changes_by_match[external_id][site_key] = {
                        "old": old_odds,
                        "new": odds_1x2,
                        "match_name": match_info.get("match_name", ""),
                        "site_key": site_key
                    }
                
                # ✅ BUG FIX #3: Update cache AFTER comparison
                if external_id not in self.daily_combinaison_cache:
                    self.daily_combinaison_cache[external_id] = {}
                self.daily_combinaison_cache[external_id][site_key] = odds_1x2

        if not changes_by_match:
            print(
                "✅ Aucun changement de cotes détecté (aucune feuille DailyCombinaison modifiée)")
            return

        print(
            f"🔄 {sum(len(v) for v in changes_by_match.values())} changement(s) détecté(s), MAJ incrémentale...")
        # 3. Re-bâtir l'index combos avec le nouveau cache RAM
        combinations_index = self._build_combinations_index(
            self.daily_combinaison_cache)
        # 4. Mise à jour incrémentale des feuilles concernées UNIQUEMENT
        await self._update_separate_sheets_incremental(changes_by_match, combinations_index)
        print("✅ Mise à jour incrémentale terminée.")

    async def _create_separate_sheets(self, odds_1x2_by_match: dict, combinations_index: dict):
        """Créer 4 feuilles séparées (1 colonne par match identique) - CORRECTION header dynamique"""

        # ✅ NOUVEAU : Charger le cache des résultats depuis Google Sheets
        print("   📊 Chargement résultats depuis 1X2_FullTime...")
        results_cache = await self._build_results_cache_from_sheet()
        
        # On reconstitue le mapping des compétitions à partir des combosIndex
        competitions = {}
        for site_key in self.SITE_ORDER:
            for match_list in combinations_index[site_key].values():
                for m in match_list:
                    cid = m.get("competition_id")
                    cname = m.get("competition")
                    if cid and cname and cid not in competitions:
                        competitions[cid] = {"name": cname}

        # Préparer les données par site
        sheets_data = {
            f"DailyCombinaison_{SITES[site_key]['name']}": []
            for site_key in SITES.keys()
        }

        matches_with_combos = 0
        total_combinations = 0

        for external_id, sites_odds in odds_1x2_by_match.items():
            if not sites_odds:
                continue

            match_info = self.matches_info_archive.get(external_id, {})
            if not match_info:
                continue

            first_info = list(match_info.values())[0]
            match_principal = first_info.get("match_name", "")
            competition_id = first_info.get("competition_id")
            competition_principal = first_info.get("competition_name", "")
            heure_principal = first_info.get("start_time", "")

            # Complète le nom de compétition si manquant
            if (not competition_principal or competition_principal == "") and competition_id:
                comp_info = competitions.get(competition_id)
                if comp_info:
                    competition_principal = comp_info.get("name", "")

            match_has_combos = False

            # Pour chaque site
            for site_key, odds_string in sites_odds.items():
                site_name = SITES[site_key]["name"]
                sheet_name = f"DailyCombinaison_{site_name}"

                odds_key = odds_string.replace(" ", "")

                # Récupérer matchs identiques
                all_matches = combinations_index[site_key].get(odds_key, [])
                matching_matches = [m for m in all_matches if m.get("external_id") != external_id]

                if matching_matches:
                    match_has_combos = True
                    total_combinations += len(matching_matches)

                # Trier par heure et limiter au maximum défini
                matching_matches_sorted = sorted(
                    matching_matches,
                    key=lambda m: self._get_time_until_match(m["start_time"]) or 9999
                )[:self.MAX_MATCHING_MATCHES]

                # Créer la ligne de base
                row = {
                    "Date": now_mauritius_str("%Y-%m-%d"),
                    "Match Principal": match_principal,
                    "Compétition": competition_principal,
                    "Heure Match": heure_principal,
                    "Cotes 1X2": odds_string,
                }

                # Ajouter chaque match similaire dans sa colonne
                for i, match in enumerate(matching_matches_sorted, start=1):
                    match_name = match.get('match_name', '')
                    competition = match.get('competition', '')
                    
                    # ✅ Direct lookup by match name
                    match_result = ""
                    if match_name in results_cache.get(site_key, {}):
                        ft = results_cache[site_key][match_name]
                        match_result = f" [{ft}]"

                    # Ajouter le résultat au nom du match
                    row[f"Match {i}"] = f"{match_name} ({competition}){match_result}"
                    row[f"Heure {i}"] = match.get('start_time', '')

                row["Nombre Total"] = len(matching_matches_sorted)
                sheets_data[sheet_name].append(row)

            if match_has_combos:
                matches_with_combos += 1

        # Sauvegarder stats
        self.daily_combo_total_matches = len(odds_1x2_by_match)
        self.daily_combo_matches_with_combos = matches_with_combos
        self.daily_combo_total_combinations = total_combinations

        # ✅ NOUVEAU : Calculer le nombre MAX de matchs similaires pour créer le header dynamique
        max_matches_per_row = 0
        for sheet_name, rows in sheets_data.items():
            for row in rows:
                # Compter combien de colonnes "Match X" existent
                match_columns = [k for k in row.keys() if k.startswith("Match ")]
                if len(match_columns) > max_matches_per_row:
                    max_matches_per_row = len(match_columns)

        if max_matches_per_row > 0:
            print(f"   📊 Maximum {max_matches_per_row} matchs similaires par ligne")

        # Envoyer chaque feuille AVEC HEADER DYNAMIQUE
        for sheet_name, rows in sheets_data.items():
            if not rows:
                print(f"   ⚠️  {sheet_name} : Aucune combinaison")
                continue

            print(f"   📤 {sheet_name} : {len(rows)} ligne(s)...")

            # ✅ NOUVEAU : Créer le header dynamique
            base_header = ["Date", "Match Principal", "Compétition", "Heure Match", "Cotes 1X2"]
            
            # Ajouter colonnes pour matchs similaires
            for i in range(1, max_matches_per_row + 1):
                base_header.append(f"Match {i}")
                base_header.append(f"Heure {i}")
            
            base_header.append("Nombre Total")

            # ✅ NOUVEAU : Garantir que TOUTES les lignes ont TOUTES les colonnes du header
            normalized_rows = []
            for row in rows:
                normalized_row = {}
                for col in base_header:
                    normalized_row[col] = row.get(col, "")  # Remplir avec "" si absent
                normalized_rows.append(normalized_row)

            # Envoyer par batch
            batch_size = 500
            for i in range(0, len(normalized_rows), batch_size):
                batch = normalized_rows[i:i+batch_size]
                await self.gsheets.append_rows_batch({sheet_name: batch})

                if i + batch_size < len(normalized_rows):
                    await asyncio.sleep(5)

            print(f"      ✅ {sheet_name} : Envoyé")

        print(f"\n   ✅ 4 feuilles DailyCombinaison créées")

    def _parse_getsport_match_data(self, match_str: str, competitions: dict) -> Optional[dict]:
        """
        Parser un match GetSport + compléter systématiquement le nom de compétition.
        """
        try:
            parts = match_str.split(";")
            if len(parts) < 29:
                return None

            match_id = int(parts[0])
            competition_id = int(parts[1])
            external_id_str = parts[28]
            if not external_id_str or not external_id_str.strip():
                return None
            try:
                external_id = int(external_id_str)
                if external_id == 0:
                    return None
            except ValueError:
                return None

            # Compléter le nom de compétition à partir du mapping 'competitions'
            competition_info = competitions.get(competition_id)
            competition_name = competition_info.get(
                "name", "") if competition_info else ""
            category = competition_info.get(
                "country", "") if competition_info else ""

            return {
                "match_id": match_id,
                "competition_id": competition_id,
                "external_id": external_id,
                "match_name": parts[2],
                "start_time": parts[3],
                "competition_name": competition_name,
                "category": category,
                "market_count": int(parts[14]) if len(parts) > 14 and parts[14].isdigit() else 0,
                "sport_id": "soccer",
                "raw_match_str": match_str
            }
        except Exception:
            return None

    async def get_sport_page(self, site_key: str, date: str, page_no: int, inclusive: int = 0) -> Optional[dict]:
        """Récupérer une page via GetSport"""
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/GetSport"
        params = {
            "sportId": "soccer",
            "date": f"{date}T00:00:00+04:00",
            "category": "",
            "competitionId": 0,
            "pageNo": page_no,
            "inclusive": inclusive,
            "matchid": 0,
            "periodCode": "today"
        }
        return await self.fetch(url, params, SITES[site_key]["name"])

    async def get_all_matches_for_site_date(self, site_key: str, date: str) -> Dict[int, dict]:
        """Récupérer TOUS les matchs d'un site pour une date avec pagination parallèle,
        avec un mapping global des compétitions
        """

        page1_data = await self.get_sport_page(site_key, date, 1, inclusive=1)
        if not page1_data:
            return {}

        # 1. *** Parsing GLOBAL competitionData (mapping) ***
        competitions = {}
        comp_data = page1_data.get("competitionData", "")
        if comp_data:
            for comp_str in comp_data.split("|"):
                if not comp_str.strip():
                    continue
                parts = comp_str.split(";")
                if len(parts) >= 6:
                    comp_id = int(parts[0])
                    competitions[comp_id] = {
                        "id": comp_id,
                        "name": parts[1],
                        "country": parts[5]
                    }

        # 2. On parcourt toutes les pages EN PARALLELE
        all_matches = {}
        total_pages = page1_data.get("totalPages", 1)
        page_datas = [page1_data]

        if total_pages > 1:
            tasks = [
                self.get_sport_page(site_key, date, page_no, inclusive=0)
                for page_no in range(2, total_pages + 1)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # On récupère les competitionData rencontrés pour chaque page !
            for page_data in results:
                if isinstance(page_data, Exception) or not page_data:
                    continue
                page_datas.append(page_data)
                extra_comp_data = page_data.get("competitionData", "")
                if extra_comp_data:
                    for comp_str in extra_comp_data.split("|"):
                        if not comp_str.strip():
                            continue
                        parts = comp_str.split(";")
                        if len(parts) >= 6:
                            comp_id = int(parts[0])
                            if comp_id not in competitions:
                                competitions[comp_id] = {
                                    "id": comp_id,
                                    "name": parts[1],
                                    "country": parts[5]
                                }

        for page_data in page_datas:
            match_data = page_data.get("matchData", "")
            if match_data:
                for match_str in match_data.split("|"):
                    if not match_str.strip():
                        continue
                    match_info = self._parse_getsport_match_data(
                        match_str, competitions)
                    if match_info:
                        match_info["site"] = site_key
                        all_matches[match_info["external_id"]] = match_info

        return all_matches

    async def get_all_sites_matches(self, date: str) -> Dict[str, Dict[int, dict]]:
        """Récupérer TOUS les matchs des 4 sites avec monitoring"""

        tasks = [
            self.get_all_matches_for_site_date(site_key, date)
            for site_key in SITES.keys()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        matches_by_site = {}
        total_errors = 0

        for site_key, result in zip(SITES.keys(), results):
            if isinstance(result, Exception):
                matches_by_site[site_key] = {}
                error_msg = f"{type(result).__name__}"
                print(f"   ❌ {SITES[site_key]['name']:15s}: {error_msg}")
                self.api_health.record_getsport(site_key, False, error_msg)

            elif result is None or len(result) == 0:
                matches_by_site[site_key] = {}
                error_msg = "Aucun match retourné"
                print(f"   ⚠️  {SITES[site_key]['name']:15s}: {error_msg}")
                self.api_health.record_getsport(site_key, False, error_msg)

            else:
                matches_by_site[site_key] = result
                print(
                    f"   ✅ {SITES[site_key]['name']:15s}: {len(result):3d} matchs")
                self.api_health.record_getsport(site_key, True)

        return matches_by_site

    async def get_match_details(self, site_key: str, sport_id: str, competition_id: int, match_id: int):
        """Récupérer les détails d'un match via GetMatch"""
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/GetMatch"
        params = {
            "sportId": sport_id.lower(),
            "competitionId": competition_id,
            "matchId": match_id,
            "periodCode": "all"
        }
        return await self.fetch(url, params, SITES[site_key]["name"])

    def extract_odds(self, data: dict, site_key: str) -> dict:
        """Extraire les cotes d'une réponse GetMatch"""
        odds = {"site": site_key, "timestamp": now_mauritius_iso(),
                "markets": {}}
        try:
            competitions = data.get("competitions", [])
            if not competitions:
                return odds
            matches = competitions[0].get("matches", [])
            if not matches:
                return odds
            match_data = matches[0]
            markets = match_data.get("markets", [])

            competition_name = competitions[0].get("name", "")
            if competition_name:
                odds["competition_name"] = competition_name

            for market in markets:
                market_code = market.get("marketCode", "")
                market_period = market.get("periodCode", "")
                market_line = market.get("marketLine", "")
                market_display = market.get("marketDisplayName", "")
                key = f"{market_code}_{market_period}_{market_line}".strip("_")
                selections = market.get("selectionList", [])
                market_odds = {}

                for sel in selections:
                    option_code = sel.get("optionCode", "")
                    option_name = sel.get("name", "")
                    odd_value = self.safe_float(sel.get("companyOdds"), 0.0)
                    dict_key = option_code if option_code else option_name
                    market_odds[dict_key] = {
                        "name": option_name,
                        "code": option_code,
                        "odd": odd_value,
                        "option_no": sel.get("optionNo", ""),
                    }

                odds["markets"][key] = {
                    "display_name": market_display,
                    "period": market_period,
                    "line": market_line,
                    "market_code": market_code,
                    "odds": market_odds,
                }
        except Exception as e:
            pass
        return odds

    def _get_close_matches(self, window_minutes: int = 60) -> List[int]:
        """Identifier les matchs proches du début"""
        close_matches = []

        all_match_ids = set(self.matches_by_external_id.keys()) | set(
            self.matches_info_archive.keys())

        for external_id in all_match_ids:
            if external_id in self.completed_matches:
                continue

            match_info_dict = self.matches_by_external_id.get(
                external_id) or self.matches_info_archive.get(external_id)
            if not match_info_dict:
                continue

            first_match = list(match_info_dict.values())[0]
            start_time = first_match.get("start_time", "")

            time_until_match = self._get_time_until_match(start_time)

            if time_until_match is None:
                continue

            if -5 <= time_until_match <= window_minutes:
                close_matches.append(external_id)

        return close_matches

    async def verify_close_matches_availability(self):
        """Vérifier la disponibilité des matchs proches avec statistiques"""

        close_matches = self._get_close_matches(CLOSE_MATCH_WINDOW_MINUTES)

        if not close_matches:
            return

        stats = {
            "total_attempts": 0,
            "success": 0,
            "updated": 0,
            "closed": 0,
            "failed": 0,
            "errors_by_site": defaultdict(int)
        }

        capture_count = 0

        for external_id in close_matches:
            match_info_dict = self.matches_by_external_id.get(
                external_id) or self.matches_info_archive.get(external_id)
            if not match_info_dict:
                continue

            open_sites = set(match_info_dict.keys()) - \
                self.closed_sites[external_id]

            if not open_sites:
                continue

            for site_key in open_sites:
                match_info = match_info_dict[site_key]
                stats["total_attempts"] += 1

                try:
                    data = await self.get_match_details(
                        site_key,
                        match_info["sport_id"],
                        match_info["competition_id"],
                        match_info["match_id"]
                    )

                    if data is None:
                        # Match fermé
                        self.closed_sites[external_id].add(site_key)
                        await self.capture_odds_for_sites(external_id, {site_key}, force_refresh=True)
                        stats["closed"] += 1

                        self.api_health.record_getmatch(
                            site_key, False, "Match fermé")

                    else:
                        odds = self.extract_odds(data, site_key)

                        if odds and odds.get("markets"):
                            if "competition_name" in odds and odds["competition_name"]:
                                match_info["competition_name"] = odds["competition_name"]

                            is_update = external_id in self.captured_odds and site_key in self.captured_odds[
                                external_id]

                            # Détecter changement de cotes
                            if is_update:
                                old_odds = self.captured_odds[external_id][site_key]
                                has_changed = self._odds_have_changed(
                                    old_odds, odds)

                                if has_changed:
                                    print(
                                        f"      🔄 {SITES[site_key]['name']}: Cotes MODIFIÉES ({len(odds['markets'])} marchés) 📊")
                                else:
                                    print(
                                        f"      ✓ {SITES[site_key]['name']}: Cotes identiques ({len(odds['markets'])} marchés)")

                            self.captured_odds[external_id][site_key] = odds

                            if is_update:
                                stats["updated"] += 1
                            else:
                                capture_count += 1
                                stats["success"] += 1

                            # Sauvegarde (throttled)
                            self.save_cache_to_disk()

                            self.api_health.record_getmatch(site_key, True)
                        else:
                            stats["failed"] += 1
                            self.api_health.record_getmatch(
                                site_key, False, "Pas de marchés")

                except Exception as e:
                    stats["failed"] += 1
                    stats["errors_by_site"][site_key] += 1
                    error_msg = f"{type(e).__name__}"
                    self.api_health.record_getmatch(site_key, False, error_msg)

        if capture_count > 0:
            print(f"   ✅ Capture dynamique : {capture_count} nouveaux sites")

        if stats["total_attempts"] > 0:
            print(f"\n   📊 GetMatch: {stats['total_attempts']} tentatives")
            if stats["success"] > 0:
                print(f"      ✅ Nouveaux : {stats['success']}")
            if stats["updated"] > 0:
                print(f"      🔄 Actualisés : {stats['updated']}")
            if stats["closed"] > 0:
                print(f"      🔒 Fermés : {stats['closed']}")
            if stats["failed"] > 0:
                print(f"      ❌ Erreurs : {stats['failed']}")

            if stats["errors_by_site"]:
                print(f"      📉 Erreurs par site :")
                for site_key, count in stats["errors_by_site"].items():
                    print(
                        f"         • {SITES[site_key]['name']:15s}: {count} erreur(s)")

    async def detect_early_closures_and_reopenings(self, current_matches_by_site: Dict[str, Dict[int, dict]]):
        """Détecter fermetures ET capturer IMMÉDIATEMENT avant disparition"""

        current_snapshot = defaultdict(dict)
        for site_key, matches in current_matches_by_site.items():
            for external_id in matches.keys():
                current_snapshot[external_id][site_key] = True

        for external_id in list(self.early_closed.keys()):
            if external_id in self.completed_matches:
                continue

            early_closed_sites = self.early_closed[external_id]
            current_sites = set(current_snapshot.get(external_id, {}).keys())

            reopened_sites = early_closed_sites & current_sites

            if reopened_sites:
                match_info = self.matches_info_archive.get(external_id)
                if match_info:
                    first_match = list(match_info.values())[0]
                    match_name = first_match.get("match_name", "")

                    print(f"\n   🟢 RÉAPPARITION après fermeture précoce")
                    print(f"      📌 Match {external_id} : {match_name[:40]}")
                    print(
                        f"      ✅ Sites : {', '.join([SITES[s]['name'] for s in reopened_sites])}")

                self.early_closed[external_id] -= reopened_sites

                if not self.early_closed[external_id]:
                    del self.early_closed[external_id]

        for external_id in self.matches_snapshot.keys():
            if external_id in self.completed_matches:
                continue

            previous_sites = set(self.matches_snapshot[external_id].keys())
            current_sites = set(current_snapshot.get(external_id, {}).keys())

            disappeared_sites = previous_sites - current_sites - \
                self.closed_sites[external_id] - \
                self.early_closed.get(external_id, set())

            if disappeared_sites:
                match_info = self.matches_info_archive.get(external_id)
                if not match_info:
                    continue

                first_match = list(match_info.values())[0]
                match_name = first_match.get("match_name", "")
                start_time = first_match.get("start_time", "")
                time_until_match = self._get_time_until_match(start_time)

                if time_until_match is None:
                    continue

                print(f"\n   🔴 Fermeture détectée (GetSport)")
                print(f"      📌 Match {external_id} : {match_name[:40]}")
                print(
                    f"      🕐 Temps avant match : {time_until_match:.0f} min")
                print(
                    f"      ❌ Sites : {', '.join([SITES[s]['name'] for s in disappeared_sites])}")

                if time_until_match > 60:
                    print(f"      ⚠️  Fermeture PRÉCOCE (> 1h avant)")
                    print(f"      📸 Tentative capture AVANT disparition...")

                    await self.capture_odds_for_sites(external_id, disappeared_sites, force_refresh=True)

                    for site_key in disappeared_sites:
                        self.early_closed[external_id].add(site_key)

                else:
                    print(f"      📸 Capture des cotes...")

                    await self.capture_odds_for_sites(external_id, disappeared_sites, force_refresh=True)
                    self.closed_sites[external_id].update(disappeared_sites)

        self.matches_snapshot = current_snapshot
        # Sauvegarde (throttled) après mise à jour d'état
        self.save_cache_to_disk()

    async def capture_odds_for_sites(self, external_id: int, site_keys: Set[str], force_refresh: bool = False):
        """Capturer les cotes avec mise à jour continue pour matchs proches"""
        tasks = []
        sites_to_capture = []

        for site_key in site_keys:
            match_info = None
            if external_id in self.matches_by_external_id and site_key in self.matches_by_external_id[external_id]:
                match_info = self.matches_by_external_id[external_id][site_key]
            elif external_id in self.matches_info_archive and site_key in self.matches_info_archive[external_id]:
                match_info = self.matches_info_archive[external_id][site_key]

            if not match_info:
                continue

            # VÉRIFIER SI MATCH PROCHE (< 60 min)
            start_time = match_info.get("start_time", "")
            time_until = self._get_time_until_match(start_time)

            is_close_match = (time_until is not None and time_until <= 60)

            has_cache = external_id in self.captured_odds and site_key in self.captured_odds[
                external_id]

            # DÉCISION : Capturer si...
            should_capture = (
                force_refresh or                    # Force demandé
                not has_cache or                    # Pas de cache
                is_close_match                      # Match proche → UPDATE continu
            )

            if not should_capture:
                continue

            # LOG différent selon situation
            if has_cache and is_close_match:
                old_timestamp = self.captured_odds[external_id][site_key].get(
                    "timestamp", "")
                try:
                    old_dt = date_parser.parse(old_timestamp)
                    age_minutes = (now_mauritius() -
                                   old_dt).total_seconds() / 60
                    print(
                        f"         🔄 {SITES[site_key]['name']}: Mise à jour cotes (dernière capture il y a {age_minutes:.0f} min)")
                except:
                    print(
                        f"         🔄 {SITES[site_key]['name']}: Mise à jour cotes")

            task = self.get_match_details(
                site_key,
                match_info["sport_id"],
                match_info["competition_id"],
                match_info["match_id"]
            )
            tasks.append(task)
            sites_to_capture.append(site_key)

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for site_key, result in zip(sites_to_capture, results):
            has_cache = external_id in self.captured_odds and site_key in self.captured_odds[
                external_id]

            if isinstance(result, Exception) or not result:
                if has_cache:
                    print(
                        f"         ⚠️  {SITES[site_key]['name']}: Échec update → conservation cache")
                else:
                    print(
                        f"         ❌ {SITES[site_key]['name']}: Pas de cotes (déjà disparu)")

                error_msg = f"{type(result).__name__}" if isinstance(
                    result, Exception) else "Pas de réponse"
                self.api_health.record_getmatch(site_key, False, error_msg)
                continue

            odds = self.extract_odds(result, site_key)

            if odds and odds.get("markets"):
                was_updated = has_cache

                self.captured_odds[external_id][site_key] = odds

                if was_updated:
                    print(
                        f"         ✅ {SITES[site_key]['name']}: {len(odds['markets'])} marchés ACTUALISÉS !")
                else:
                    print(
                        f"         ✅ {SITES[site_key]['name']}: {len(odds['markets'])} marchés CAPTURÉS !")

                # Sauvegarde (throttled)
                self.save_cache_to_disk()

                self.api_health.record_getmatch(site_key, True)
            else:
                self.api_health.record_getmatch(
                    site_key, False, "Pas de marchés")

    async def check_matches_for_finalization(self):
        """Vérifier quels matchs doivent être finalisés (INCLUANT les déjà commencés)"""

        all_match_ids = set(self.matches_by_external_id.keys()) | set(
            self.matches_info_archive.keys())

        for external_id in all_match_ids:
            if external_id in self.completed_matches:
                continue

            all_sites_for_match = set()
            if external_id in self.matches_by_external_id:
                all_sites_for_match = set(
                    self.matches_by_external_id[external_id].keys())
            elif external_id in self.matches_info_archive:
                all_sites_for_match = set(
                    self.matches_info_archive[external_id].keys())

            if not all_sites_for_match:
                continue

            all_closed = self.closed_sites[external_id] | self.early_closed.get(
                external_id, set())

            if all_closed >= all_sites_for_match:
                match_info = self.matches_info_archive.get(external_id)
                if not match_info:
                    continue

                first_match = list(match_info.values())[0]
                start_time = first_match.get("start_time", "")
                time_until = self._get_time_until_match(start_time)

                if time_until is not None and time_until <= 0:
                    priority = "urgent" if time_until < 5 else "normal"

                    # Anti double-finalisation: ne pas re-queue si déjà en cours ou déjà dans la file
                    if external_id in self.finalizing_in_progress or external_id in self.finalization_queue:
                        continue

                    await self.finalization_queue.add_match(external_id, priority)

    async def finalize_multiple_matches_batch(self, external_ids: List[int]):
        """Finaliser plusieurs matchs + gestion matchs sans cotes"""

        all_sheets_data = defaultdict(list)

        for external_id in external_ids:
            # VÉRIFIER D'ABORD LE CACHE
            has_cached_odds = (external_id in self.captured_odds and
                               len(self.captured_odds[external_id]) > 0)

            if not has_cached_odds:
                # Vraiment aucune cote (ni cache ni nouveau)
                print(
                    f"      ⚠️  Match {external_id} : Aucune cote capturée → ajouté en retry")

                # Stocker pour retry au lieu de supprimer
                matches_info = self.matches_by_external_id.get(
                    external_id) or self.matches_info_archive.get(external_id)

                if matches_info:
                    first_match = list(matches_info.values())[0]

                    if external_id not in self.matches_without_odds_retry:
                        self.matches_without_odds_retry[external_id] = {
                            "match_info": matches_info,
                            "retry_count": 0,
                            "last_attempt": now_mauritius(),
                            "match_name": first_match.get("match_name", ""),
                            "start_time": first_match.get("start_time", "")
                        }
                    else:
                        self.matches_without_odds_retry[external_id]["retry_count"] += 1

                # Sauvegarde (throttled)
                self.save_cache_to_disk()
                continue

            # On a des cotes en cache → Utiliser pour Google Sheets
            cached_sites = list(self.captured_odds[external_id].keys())
            print(
                f"      ✅ Match {external_id} : {len(cached_sites)} site(s) avec cotes (cache)")

            matches_info = None
            if external_id in self.matches_by_external_id and self.matches_by_external_id[external_id]:
                matches_info = self.matches_by_external_id[external_id]
            elif external_id in self.matches_info_archive:
                matches_info = self.matches_info_archive[external_id]

            if not matches_info:
                continue

            try:
                sheets_data = self._prepare_sheets_data(
                    external_id, matches_info)

                if not sheets_data:
                    continue

                for sheet_name, rows in sheets_data.items():
                    all_sheets_data[sheet_name].extend(rows)

            except Exception:
                pass

        # Si aucune donnée, sortir
        if not all_sheets_data:
            print(f"      ⚠️  Aucune donnée à envoyer dans ce batch")
            return

        try:
            gsheets_success = await self.gsheets.append_rows_batch(dict(all_sheets_data))

            self.gsheets.update_summary(force=False)

            self._write_to_local_excel(dict(all_sheets_data))

            if gsheets_success:
                for external_id in external_ids:
                    # Seulement les matchs AVEC cotes
                    if external_id in self.captured_odds and len(self.captured_odds[external_id]) > 0:
                        self.completed_matches.add(external_id)

                        if external_id in self.matches_info_archive:
                            del self.matches_info_archive[external_id]
                        if external_id in self.captured_odds:
                            del self.captured_odds[external_id]
                        if external_id in self.closed_sites:
                            del self.closed_sites[external_id]
                # Sauvegarde immédiate après finalisation
                self.save_cache_to_disk(force=True)

        except Exception as e:
            print(f"      ❌ Erreur finalisation batch: {e}")

    # PATCH: Ajout colonne résultat par marché dans Google Sheets

    # ⏩ Modifie ta méthode _prepare_sheets_data pour ajouter le score correspondant au marché sur chaque onglet
    def _prepare_sheets_data(self, external_id: int, matches_info: Dict[str, dict]) -> Dict[str, List[Dict]]:
        """Préparer données pour Google Sheets AVEC colonnes matchID et résultats -- alignement strict! """

        first_match = list(matches_info.values())[0]

        competition_name = first_match.get("competition_name", "")
        if not competition_name and external_id in self.captured_odds:
            for site_odds in self.captured_odds[external_id].values():
                if "competition_name" in site_odds and site_odds["competition_name"]:
                    competition_name = site_odds["competition_name"]
                    break

        # Données de base (6 premières colonnes)
        base_data = {
            "Date": now_mauritius_str("%Y-%m-%d"),
            "Heure": now_mauritius_str("%H:%M:%S"),
            "External ID": external_id,
            "Match": first_match["match_name"],
            "Compétition": competition_name,
            "Heure Match": first_match["start_time"],
        }

        all_market_keys = set()
        for site_odds in self.captured_odds[external_id].values():
            all_market_keys.update(site_odds.get("markets", {}). keys())

        sheets_data = {}

        for market_key in all_market_keys:
            sheet_name = MARKET_SHEET_MAPPING.get(market_key, market_key[:31])
            
            # ✅ CRITIQUE : Construire les données de ligne avec ordre EXPLICITE
            row_data = base_data.copy()

            # Ajouter les colonnes de cotes dans l'ORDRE EXACT (en utilisant SITE_ORDER)
            for site_key in self.SITE_ORDER:
                site_name = SITES[site_key]["name"]

                if site_key in self.captured_odds[external_id]:
                    markets = self. captured_odds[external_id][site_key].get("markets", {})
                    if market_key in markets:
                        odds_str = self._format_odds_for_display(
                            markets[market_key]. get("odds", {}))
                        row_data[site_name] = odds_str
                    else:
                        row_data[site_name] = ""
                else:
                    row_data[site_name] = ""

            # Ajouter les colonnes matchID dans l'ORDRE EXACT (en utilisant SITE_ORDER)
            for site_key in self.SITE_ORDER:
                site_name = SITES[site_key]["name"]
                match_info = matches_info.get(site_key)
                if match_info:
                    row_data[f"matchID_{site_name}"] = match_info.get("match_id", "")
                else:
                    row_data[f"matchID_{site_name}"] = ""
            
            # Ajouter la colonne résultat selon le nom de la feuille (APRÈS cotes et matchID)
            if sheet_name == "1X2_FullTime":
                row_data["Résultat_FullTime"] = ""
            elif sheet_name == "1X2_HalfTime":
                row_data["Résultat_HalfTime"] = ""
            elif sheet_name == "1X2_2ndHalf":
                row_data["Résultat_SecondHalf"] = ""
            elif "OverUnder" in sheet_name and "FT" in sheet_name:
                row_data["Résultat_FullTime"] = ""
            elif "OverUnder" in sheet_name and "HT" in sheet_name:
                row_data["Résultat_HalfTime"] = ""

            # ✅ CRITIQUE : Définir le header COMPLET avec TOUTES les colonnes dans l'ordre EXACT
            header = [
                "Date", "Heure", "External ID", "Match", "Compétition", "Heure Match",
                "StevenHills", "SuperScore", "ToteLePEP", "PlayOnline",
                "matchID_StevenHills", "matchID_SuperScore", "matchID_ToteLePEP", "matchID_PlayOnline",
            ]
            
            # Ajouter la colonne résultat au header selon le nom de la feuille
            if sheet_name == "1X2_FullTime":
                header.append("Résultat_FullTime")
            elif sheet_name == "1X2_HalfTime":
                header.append("Résultat_HalfTime")
            elif sheet_name == "1X2_2ndHalf":
                header. append("Résultat_SecondHalf")
            elif "OverUnder" in sheet_name and "FT" in sheet_name:
                header.append("Résultat_FullTime")
            elif "OverUnder" in sheet_name and "HT" in sheet_name:
                header.append("Résultat_HalfTime")

            # ✅ CRITIQUE : Convertir en dict ordonné correspondant EXACTEMENT au header
            final_row_dict = {col: row_data. get(col, "") for col in header}
            
            # ✅ VALIDATION : Vérifier l'alignement
            if set(final_row_dict.keys()) != set(header):
                missing = set(header) - set(final_row_dict.keys())
                extra = set(final_row_dict.keys()) - set(header)
                print(f"⚠️  Décalage de colonnes dans {sheet_name}:")
                if missing:
                    print(f"    Manquantes: {missing}")
                if extra:
                    print(f"    En trop: {extra}")

            if sheet_name not in sheets_data:
                sheets_data[sheet_name] = []
            sheets_data[sheet_name].append(final_row_dict)

        return sheets_data
    

    def _validate_column_alignment(self, sheet_name: str, header: List[str], rows: List[Dict]) -> bool:
        """Valider que toutes les lignes ont les bonnes colonnes dans le bon ordre"""
        for i, row in enumerate(rows):
            if list(row.keys()) != header:
                print(f"❌ Erreur d'alignement dans {sheet_name}, ligne {i+1}:")
                print(f"   Attendu: {header}")
                print(f"   Obtenu:  {list(row.keys())}")
                
                # Afficher les différences
                missing = [col for col in header if col not in row]
                extra = [col for col in row if col not in header]
                if missing:
                    print(f"   Colonnes manquantes: {missing}")
                if extra:
                    print(f"   Colonnes en trop: {extra}")
                return False
        return True

    async def update_match_results_in_sheets(self):
        """Vérifier et mettre à jour les résultats par DATE (gestion matchs tardifs)"""

        print(f"\n{'='*70}")
        print(f"🏆 VÉRIFICATION RÉSULTATS DES MATCHS (PAR DATE)")
        print(f"{'='*70}")

        # Feuilles à mettre à jour
        sheets_to_update = [
            ("1X2_FullTime", "FullTime", "fullTime"),
            ("1X2_HalfTime", "HalfTime", "halfTime"),
            ("1X2_2ndHalf", "SecondHalf", "secondHalfTime")
        ]

        total_updates = 0

        for sheet_name, result_suffix, json_key in sheets_to_update:
            worksheet = self.gsheets.get_or_create_worksheet(sheet_name)
            if not worksheet:
                continue

            print(f"\n   📝 {sheet_name}...")

            def get_all():
                return worksheet.get_all_values()

            all_values = self.gsheets._execute_with_retry(get_all)

            if not all_values or len(all_values) < 2:
                continue

            header = all_values[0]

            # Trouver colonnes nécessaires
            try:
                col_date = header.index("Date")
                col_result = header.index(f"Résultat_{result_suffix}")
            except ValueError:
                print(f"      ⚠️ Colonnes manquantes (Date ou Résultat)")
                continue

            # Trouver colonnes matchID
            matchid_cols = {}
            for site_key in self.SITE_ORDER:
                site_name = SITES[site_key]["name"]
                try:
                    matchid_cols[site_key] = header.index(f"matchID_{site_name}")
                except ValueError:
                    matchid_cols[site_key] = None

            # ✅ NOUVEAU : Grouper lignes par date + vérifier si résultats manquants
            dates_needing_results = {}  # {date: [row_indices]}

            for row_index, row in enumerate(all_values[1:], start=2):
                if len(row) <= max(col_date, col_result):
                    continue

                date_str = row[col_date] if col_date < len(row) else ""
                existing_result = row[col_result] if col_result < len(row) else ""

                # Skip si résultat déjà rempli
                if existing_result and existing_result not in ["", "C"]:
                    continue

                # Vérifier s'il y a AU MOINS 1 matchID
                has_match_id = False
                for site_key in self.SITE_ORDER:
                    col_idx = matchid_cols.get(site_key)
                    if col_idx is not None and len(row) > col_idx:
                        match_id_str = row[col_idx]
                        if match_id_str and match_id_str.strip():
                            has_match_id = True
                            break

                if has_match_id and date_str:
                    if date_str not in dates_needing_results:
                        dates_needing_results[date_str] = []
                    dates_needing_results[date_str].append(row_index)

            if not dates_needing_results:
                print(f"      ✅ Tous les résultats sont à jour")
                continue

            print(f"      📅 {len(dates_needing_results)} date(s) avec résultats manquants")

            # ✅ NOUVEAU : Trier les dates par ordre chronologique
            sorted_dates = sorted(dates_needing_results.keys())

            # ✅ NOUVEAU : Pour chaque date, faire 1 POST et compléter
            for date_str in sorted_dates:
                row_indices = dates_needing_results[date_str]
                
                print(f"\n      📆 Traitement date {date_str} ({len(row_indices)} ligne(s))...")

                # Convertir date format YYYY-MM-DD vers DD/MM/YYYY
                try:
                    from datetime import datetime
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    date_formatted = date_obj.strftime("%d/%m/%Y")
                except:
                    print(f"         ⚠️ Format de date invalide : {date_str}")
                    continue

                # Récupérer résultats des 4 sites pour cette date
                tasks = [
                    self.fetch_match_results(site_key, date_formatted)
                    for site_key in SITES. keys()
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Indexer par site et matchId
                results_by_site = {}

                for site_key, result in zip(SITES.keys(), results):
                    if isinstance(result, Exception) or not result or not result.get("isSuccess"):
                        results_by_site[site_key] = {}
                        continue

                    results_by_site[site_key] = {}

                    for country in result.get("transaction", []):
                        for match in country.get("matches", []):
                            match_id = match.get("matchId")
                            if match_id:
                                results_by_site[site_key][match_id] = {
                                    "fullTime": match.get("fullTime", ""),
                                    "halfTime": match.get("halfTime", ""),
                                    "secondHalfTime": match.get("secondHalfTime", "")
                                }

                total_results_for_date = sum(len(r) for r in results_by_site.values())

                if total_results_for_date == 0:
                    print(f"         ⚠️ Aucun résultat disponible pour {date_formatted}")
                    continue

                print(f"         ✅ {total_results_for_date} résultats récupérés")

                # Mettre à jour les lignes de cette date
                updates_made = 0

                for row_index in row_indices:
                    row = all_values[row_index - 1]  # -1 car all_values inclut header

                    # Skip si résultat déjà rempli (double check)
                    existing_result = row[col_result] if col_result < len(row) else ""
                    if existing_result and existing_result not in ["", "C"]:
                        continue

                    # Chercher résultat dans les 4 sites
                    result_found = None

                    for site_key in self.SITE_ORDER:
                        col_idx = matchid_cols.get(site_key)
                        if col_idx is None or len(row) <= col_idx:
                            continue

                        match_id_str = row[col_idx]
                        if not match_id_str:
                            continue

                        try:
                            match_id = int(match_id_str)
                        except ValueError:
                            continue

                        # Chercher résultat
                        if site_key in results_by_site and match_id in results_by_site[site_key]:
                            result_data = results_by_site[site_key][match_id]
                            result_found = result_data.get(json_key, "")

                            if result_found and result_found not in ["", "C", None]:
                                break

                    # Mettre à jour si trouvé
                    if result_found and result_found not in ["", "C", None]:
                        try:
                            col_letter = chr(65 + col_result)

                            def update_cell():
                                return worksheet.update(
                                    values=[[result_found]],
                                    range_name=f"{col_letter}{row_index}"
                                )

                            self.gsheets._execute_with_retry(update_cell)
                            updates_made += 1
                            total_updates += 1

                            await asyncio.sleep(1)
                        except Exception as e:
                            print(f"         ❌ Erreur update ligne {row_index}: {e}")
                            continue

                if updates_made > 0:
                    print(f"         ✅ {updates_made} résultat(s) ajouté(s) pour {date_str}")

        print(f"\n{'='*70}")
        print(f"✅ Total: {total_updates} résultat(s) ajouté(s)")
        print(f"{'='*70}\n")


    async def fetch_match_results(self, site_key: str, date: str) -> Optional[dict]:
        """
        Récupérer les résultats via /webapi/searchresult

        Args:
            site_key: Clé du site (ex: "stevenhills")
            date: Date format "DD/MM/YYYY"

        Returns:
            JSON avec résultats des matchs
        """
        base_url = SITES[site_key]["base_url"]
        url = f"{base_url}/webapi/searchresult"

        # POST request
        headers = HEADERS.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"

        payload = {"DateFrom": date}

        try:
            async with self.session.post(url, headers=headers, data=payload, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None
        except:
            return None

    def _write_to_local_excel(self, sheets_data: Dict[str, List[dict]]):
        """Écrire dans Excel local"""

        if self.local_cumulative_excel.exists():
            try:
                existing_sheets = pd.read_excel(
                    self.local_cumulative_excel, sheet_name=None, engine='openpyxl')
            except:
                existing_sheets = {}
        else:
            existing_sheets = {}

        all_sheet_names = set(existing_sheets.keys()) | set(sheets_data.keys())
        all_sheet_names.discard("Summary")

        with pd.ExcelWriter(self.local_cumulative_excel, engine='openpyxl', mode='w') as writer:

            summary_data = [
                ["ALL MATCHES CUMULATIVE - LOCAL"],
                ["Dernière mise à jour", now_mauritius_str()],
                ["Fuseau horaire", "Indian/Mauritius (UTC+4)"],
                ["Utilisateur", "antema102"],
                [""],
                ["Feuilles disponibles:"],
            ]

            for sheet_name in sorted(all_sheet_names):
                summary_data.append([f"  - {sheet_name}"])

            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary',
                                index=False, header=False)

            for sheet_name in sorted(all_sheet_names):
                existing_df = existing_sheets.get(sheet_name, pd.DataFrame())
                new_rows = sheets_data.get(sheet_name, [])
                new_df = pd.DataFrame(new_rows)

                combined_df = pd.concat(
                    [existing_df, new_df], ignore_index=True)

                combined_df.to_excel(
                    writer, sheet_name=sheet_name, index=False)

    def _format_odds_for_display(self, market_odds: dict) -> str:
        sorted_opts = sorted(market_odds.items(),
                             key=lambda x: x[1].get("option_no", 999))
        odds_values = [
            f"{opt_data.get('odd', 0):.2f}" for _, opt_data in sorted_opts]
        return " / ".join(odds_values)

    async def process_finalization_queue(self):
        """Processeur de queue (tâche background)"""

        while True:
            try:
                batch = await self.finalization_queue.get_next_batch()

                if batch:
                    print(f"\n{'='*70}")
                    print(f"📦 TRAITEMENT BATCH ({len(batch)} matchs)")
                    print(
                        f"   Queue restante : {len(self.finalization_queue)} matchs")
                    print(f"{'='*70}")

                    # Marquer comme en cours de finalisation
                    for mid in batch:
                        self.finalizing_in_progress.add(mid)

                    try:
                        await self.finalize_multiple_matches_batch(batch)
                    finally:
                        # Libérer les verrous même en cas d'erreur
                        for mid in batch:
                            self.finalizing_in_progress.discard(mid)

                    print(f"   ✅ Batch traité")

                await asyncio.sleep(1)

            except Exception as e:
                print(f"❌ Erreur processeur queue: {e}")
                await asyncio.sleep(5)

    async def reset_daily_combinaison_sheets(self):
        """Efface les 4 feuilles et reconstruit les combos du nouveau jour uniquement"""
        print("🧹 Reset des 4 feuilles DailyCombinaison car nouveau jour")
        await self.clear_daily_combinaison_sheets()
        await self.build_daily_combinaison_sheets()

    async def _delayed_combo_rebuild(self):
        """
        Reconstruction DIFFÉRÉE des feuilles DailyCombinaison
        Attend 5 minutes après minuit pour avoir assez de matchs
        """
        print("⏳ Reconstruction DailyCombinaison dans 5 minutes...")
        await asyncio.sleep(300)  # 5 minutes

        print("\n🔄 Début reconstruction DailyCombinaison (nouveau jour)")

        # 1. Effacer les anciennes feuilles
        await self.clear_daily_combinaison_sheets()

        # 2. Récupérer les matchs du nouveau jour
        print(f"   📡 Récupération matchs du {self.current_date}...")
        all_matches_by_site = await self.get_all_sites_matches(self.current_date)

        total_matches = sum(len(matches)
                            for matches in all_matches_by_site.values())

        if total_matches == 0:
            print(
                f"   ⚠️  Aucun match disponible à {now_mauritius_str('%H:%M')}")
            print(f"   ⏰ Nouvelle tentative dans 30 minutes...")
            await asyncio.sleep(1800)  # 30 minutes
            await self._delayed_combo_rebuild()
            return

        print(f"   ✅ {total_matches} matchs trouvés pour le {self.current_date}")

        # 3.  Construire les feuilles normalement
        await self.build_daily_combinaison_sheets()

        print(
            f"   ✅ Feuilles DailyCombinaison reconstruites à {now_mauritius_str('%H:%M')}")

    def _check_date_change(self):
        """Vérifier changement de date avec nettoyage COMPLET"""
        current_date = now_mauritius_date_str()

        if current_date != self.current_date:
            print(f"\n{'='*70}")
            print(f"🌙 PASSAGE À MINUIT DÉTECTÉ")
            print(f"{'='*70}")
            print(f"   Date précédente : {self.current_date}")
            print(f"   Nouvelle date   : {current_date}")

            # ✅ AJOUT : Log détaillé
            print(f"   🧹 Nettoyage en cours...")
            print(
                f"      - Matchs en archive : {len(self.matches_info_archive)}")
            print(
                f"      - Cotes capturées : {sum(len(s) for s in self.captured_odds.values())}")
            print(
                f"      - Cache combo : {len(self.daily_combinaison_cache)} matchs")

            # ✅ Nettoyage COMPLET (matchs + cotes + combo)
            self.matches_by_external_id.clear()
            self.captured_odds.clear()
            self.closed_sites.clear()
            self.completed_matches.clear()
            self.matches_info_archive.clear()
            self.matches_snapshot.clear()
            self.early_closed.clear()
            self.finalizing_in_progress.clear()

            # ✅ AJOUT CRITIQUE : Vider le cache combo
            old_combo_count = len(self.daily_combinaison_cache)
            self.daily_combinaison_cache.clear()

            print(f"   ✅ {old_combo_count} matchs retirés du cache combo")
            print(f"   ✅ Système réinitialisé pour le {current_date}")

            # ✅ AJOUT : Réinitialiser l'index des combinaisons
            self.combinations_index_cache = {
                site_key: {} for site_key in SITES.keys()}
            self.combinations_index_cache["last_rebuild"] = None

            self.current_date = current_date

            print(
                f"   🧹 Cache combo vidé : {old_combo_count} matchs du {self.current_date}")
            print(f"   ✅ Système réinitialisé pour le {current_date}")
            print(f"{'='*70}\n")

            # Sauvegarde immédiate
            self.save_cache_to_disk(force=True)

            # ✅ MODIFICATION : Attendre avant de reconstruire les feuilles
            asyncio.create_task(self._delayed_combo_rebuild())

    async def retry_matches_without_odds(self):
        """Retenter la capture des matchs sans cotes"""

        if not self.matches_without_odds_retry:
            return

        print(
            f"\n   🔁 Retry matchs sans cotes ({len(self.matches_without_odds_retry)} matchs)")

        matches_to_retry = list(self.matches_without_odds_retry.keys())

        for external_id in matches_to_retry:
            retry_info = self.matches_without_odds_retry[external_id]
            matches_info = retry_info["match_info"]
            retry_count = retry_info["retry_count"]

            # Abandonner si max retry ou match déjà joué
            time_until = self._get_time_until_match(retry_info["start_time"])

            if time_until is not None and time_until < -120:
                print(f"      ⏭️  Match {external_id} : Déjà joué → abandon")
                del self.matches_without_odds_retry[external_id]
                self.completed_matches.add(external_id)
                self.save_cache_to_disk()
                continue

            if retry_count >= self.max_retry_attempts:
                print(
                    f"      ❌ Match {external_id} : Max retry atteint → abandon")
                del self.matches_without_odds_retry[external_id]
                self.completed_matches.add(external_id)
                self.save_cache_to_disk()
                continue

            # Retenter capture
            sites_to_retry = set(matches_info.keys())

            for site_key in sites_to_retry:
                match_info = matches_info[site_key]
                data = await self.get_match_details(
                    site_key,
                    match_info["sport_id"],
                    match_info["competition_id"],
                    match_info["match_id"]
                )

                if data:
                    odds = self.extract_odds(data, site_key)
                    if odds and odds.get("markets"):
                        self.captured_odds[external_id][site_key] = odds
                        print(
                            f"         ✅ {SITES[site_key]['name']}: Cotes trouvées !")
                        del self.matches_without_odds_retry[external_id]
                        await self.finalization_queue.add_match(
                            external_id, "urgent")
                        self.save_cache_to_disk()
                        break

    async def resync_from_excel_to_gsheets(self):
        """Resynchroniser Google Sheets depuis Excel local"""

        print(f"\n{'='*70}")
        print(f"🔄 RESYNCHRONISATION Google Sheets depuis Excel")
        print(f"{'='*70}")

        try:
            excel_path = self.local_cumulative_excel

            if not excel_path.exists():
                print(f"❌ Fichier Excel introuvable : {excel_path}")
                return False

            print(f"📂 Lecture Excel : {excel_path.name}")

            excel_sheets = pd.read_excel(
                excel_path, sheet_name=None, engine='openpyxl')

            self.gsheets.last_row_cache.clear()

            total_synced = 0
            total_feuilles = 0

            for sheet_name, df in excel_sheets.items():
                if sheet_name == "Summary":
                    continue

                if df.empty:
                    print(f"\n   ⏭️  '{sheet_name}' : vide (ignoré)")
                    continue

                total_feuilles += 1

                # PAUSE tous les 5 feuilles
                if total_feuilles > 0 and total_feuilles % 5 == 0:
                    print(
                        f"\n   ⏸️  Pause de 60s (quota protection - {total_feuilles} feuilles traitées)...")
                    await asyncio.sleep(60)

                print(f"\n   📋 '{sheet_name}'...")

                worksheet = self.gsheets.get_or_create_worksheet(sheet_name)
                if not worksheet:
                    print(f"      ❌ Impossible de créer/obtenir la feuille")
                    continue

                try:
                    def get_all():
                        return worksheet.get_all_values()

                    gs_values = self.gsheets._execute_with_retry(get_all)
                    gs_row_count = len(gs_values)
                except Exception as e:
                    print(f"      ⚠️  Erreur lecture : {e}")
                    gs_row_count = 0

                excel_row_count = len(df) + 1

                print(f"      📊 Google Sheets : {gs_row_count} lignes")
                print(f"      📊 Excel : {excel_row_count} lignes")

                if excel_row_count > gs_row_count:
                    missing = excel_row_count - gs_row_count
                    print(f"      ⚠️  {missing} lignes manquantes !")
                    print(f"      🔄 Réécriture complète...")

                    header = list(df.columns)
                    data_rows = df.values.tolist()

                    data_rows = [
                        ['' if pd.isna(val) else val for val in row] for row in data_rows]

                    all_data = [header] + data_rows

                    try:
                        def clear_sheet():
                            return worksheet.clear()

                        self.gsheets._execute_with_retry(clear_sheet)
                        print(f"      🧹 Feuille effacée")
                    except Exception as e:
                        print(f"      ⚠️  Erreur effacement : {e}")

                    # Pause
                    await asyncio.sleep(3)

                    chunk_size = 1000

                    for i in range(0, len(all_data), chunk_size):
                        chunk = all_data[i:i+chunk_size]
                        start_row = i + 1

                        try:
                            def write_chunk():
                                range_name = f'A{start_row}'
                                return worksheet.update(values=chunk, range_name=range_name)

                            self.gsheets._execute_with_retry(write_chunk)

                            print(
                                f"      ✅ Lignes {start_row}-{start_row+len(chunk)-1} écrites")

                        except Exception as e:
                            print(f"      ❌ Erreur écriture chunk : {e}")
                            continue

                        if i + chunk_size < len(all_data):
                            await asyncio.sleep(3)

                    total_synced += len(data_rows)
                    print(f"      ✅ {len(data_rows)} lignes synchronisées")

                    await asyncio.sleep(3)

                else:
                    print(f"      ✅ Déjà synchronisé")

            print(f"\n{'='*70}")
            print(f"✅ Resynchronisation terminée")
            print(f"   📊 {total_feuilles} feuilles traitées")
            print(f"   ➕ {total_synced} lignes ajoutées au total")
            print(f"{'='*70}\n")

            # Mettre à jour Summary si quota OK
            if self.gsheets.api_call_count < 50:
                print("📝 Mise à jour Summary...")
                self.gsheets.update_summary(force=True)
            else:
                print(
                    "📝 Summary sera mis à jour lors de la prochaine itération (quota élevé)")

            return True

        except Exception as e:
            print(f"\n❌ Erreur resynchronisation : {e}")
            traceback.print_exc()
            return False

        # ==================== DAILY COMBINAISON METHODS ====================

    def _extract_1x2_full_time(self, data: dict, site_key: str) -> Optional[str]:
        """
        Extraire uniquement les cotes 1X2 Full Time (marché CP_FT_)

        Args:
            data: Réponse GetMatch
            site_key: Clé du site

        Returns:
            Format "1.20 / 3.00 / 4.00" ou None si indisponible
        """
        try:
            odds = self.extract_odds(data, site_key)
            market_key = "CP_FT_"

            if market_key not in odds.get("markets", {}):
                return None

            market_odds = odds["markets"][market_key].get("odds", {})

            # Extraire H (Home), D (Draw), A (Away)
            home_odd = market_odds.get("H", {}).get("odd", 0)
            draw_odd = market_odds.get("D", {}).get("odd", 0)
            away_odd = market_odds.get("A", {}).get("odd", 0)

            if home_odd == 0 or draw_odd == 0 or away_odd == 0:
                return None

            # Format : "1.20 / 3.00 / 4.00"
            return f"{home_odd:.2f} / {draw_odd:.2f} / {away_odd:.2f}"

        except Exception as e:
            return None

    def _extract_1x2_from_getsport_match(self, match_str: str) -> Optional[str]:
        """
        Extraire les cotes 1X2 FT DIRECTEMENT depuis matchData de GetSport

        Format matchData :
        "187111;34;CD Armenio v CS Italiano;30 Nov 00:00;0;0;CD Armenio;2. 20;Draw;2.63;CS Italiano;3.50;..."
                                                              ^^^^^      ^^^^^             ^^^^^
                                                              Home       Draw              Away

        Args:
            match_str: Ligne matchData de GetSport

        Returns:
            Format "2.20 / 2. 63 / 3.50" ou None si invalide
        """
        try:
            parts = match_str.split(";")

            if len(parts) < 12:
                return None

            # Index dans matchData (basé sur ton exemple) :
            # parts[6] = Home team name
            # parts[7] = Home odd
            # parts[8] = "Draw"
            # parts[9] = Draw odd
            # parts[10] = Away team name
            # parts[11] = Away odd

            home_odd_str = parts[7].strip()
            draw_odd_str = parts[9].strip()
            away_odd_str = parts[11].strip()

            # Convertir en float
            home_odd = float(home_odd_str)
            draw_odd = float(draw_odd_str)
            away_odd = float(away_odd_str)

            # Validate odds are in reasonable range (1.0 to 1000.0)
            if not all(1.0 <= odd <= 1000.0 for odd in [home_odd, draw_odd, away_odd]):
                return None

            # Format identique à _extract_1x2_full_time
            odds_formatted = f"{home_odd:.2f} / {draw_odd:.2f} / {away_odd:.2f}"
            
            # ✅ ENHANCEMENT #7: Validate format
            import re
            if not re.match(r'^\d+\.\d{2} / \d+\.\d{2} / \d+\.\d{2}$', odds_formatted):
                return None
            
            return odds_formatted

        except (ValueError, IndexError, AttributeError):
            return None

    def _build_combinations_index(self, odds_1x2_by_match: dict) -> dict:
        """
        Construire l'index de combinaisons PAR SITE

        IMPORTANT: Chaque site a son propre index isolé.
        StevenHills ne compare qu'avec StevenHills, etc.

        Args:
            odds_1x2_by_match: {external_id: {site: "1.20/3.00/4.00"}}

        Returns:
            {
                "stevenhills": {"1.20/3.00/4.00": [match1, match2]},
                "superscore": {"1.00/3.00/1.00": [match3]},
                ... 
            }
        """
        combinations_index = {site_key: {} for site_key in SITES.keys()}

        for external_id, sites_odds in odds_1x2_by_match.items():
            # Récupérer infos du match
            match_info = self.matches_info_archive.get(external_id, {})
            if not match_info:
                continue

            first_site_info = list(match_info.values())[0]

            for site_key, odds_string in sites_odds.items():
                # Normaliser format (enlever espaces) : "1.20/3.00/4.00"
                odds_key = odds_string.replace(" ", "")

                if odds_key not in combinations_index[site_key]:
                    combinations_index[site_key][odds_key] = []

                combinations_index[site_key][odds_key].append({
                    "external_id": external_id,
                    "match_name": first_site_info.get("match_name", ""),
                    "competition": first_site_info.get("competition_name", ""),
                    "start_time": first_site_info.get("start_time", ""),
                    "market_count": first_site_info.get("market_count", 0)
                })

        return combinations_index

    async def _load_historical_odds_from_gsheets(self) -> dict:
        """
        Charger l'historique des cotes depuis Google Sheets (feuille 1X2_FullTime)

        Returns:
            {
                "stevenhills": {
                    "1.20/3.00/4.00": [
                        {"match": "Argentine vs Chile", "date": "20 Nov 2025", "competition": "Copa America"},
                        {"match": "Brésil vs Uruguay", "date": "25 Nov 2025", "competition": "WC Qualif"}
                    ]
                },
                "superscore": { ...   }
            }
        """
        print(f"   📚 Chargement historique depuis Google Sheets (1X2_FullTime)...")

        historical_index = {site_key: {} for site_key in SITES.keys()}

        try:
            worksheet = self.gsheets.get_or_create_worksheet("1X2_FullTime")

            def get_all():
                return worksheet.get_all_values()

            all_values = self.gsheets._execute_with_retry(get_all)

            if not all_values or len(all_values) < 2:
                print(f"      ⚠️  Feuille vide ou pas de données historiques")
                return historical_index

            header = all_values[0]

            # Trouver indices des colonnes
            try:
                col_date = header.index("Date")
                col_match = header.index("Match")
                col_competition = header. index("Compétition")
                col_heure_match = header.index("Heure Match")

            except ValueError as e:
                print(f"      ❌ Colonnes manquantes : {e}")
                return historical_index

            # Indices des colonnes de sites (ex: "StevenHills", "SuperScore", etc.)
            site_columns = {}

            for site_key in self.SITE_ORDER:
                site_name = SITES[site_key]["name"]
                try:
                    site_columns[site_key] = header.index(site_name)
                except ValueError:
                    continue

            # Parser chaque ligne
            for row in all_values[1:]:  # Skip header
                if len(row) <= max(site_columns.values()):
                    continue

                date = row[col_date] if len(row) > col_date else ""
                match_name = row[col_match] if len(row) > col_match else ""
                competition = row[col_competition] if len(
                    row) > col_competition else ""
                heure_match = row[col_heure_match] if len(
                    row) > col_heure_match else ""

                if not match_name:
                    continue

                # Pour chaque site
                for site_key, col_index in site_columns.items():
                    odds_string = row[col_index] if len(
                        row) > col_index else ""

                    if not odds_string or odds_string == "":
                        continue

                    # Normaliser format (enlever espaces)
                    odds_key = odds_string.replace(" ", "").strip()

                    # Vérifier format valide (ex: "1.20/3.00/4.00")
                    if "/" not in odds_key or odds_key.count("/") != 2:
                        continue

                    # Ajouter à l'index
                    if odds_key not in historical_index[site_key]:
                        historical_index[site_key][odds_key] = []

                    historical_index[site_key][odds_key].append({
                        "match_name": match_name,
                        "date": date,
                        "competition": competition,
                        "heure_match": heure_match
                    })

            # Stats
            total_historical = sum(len(matches) for site_index in historical_index.values(
            ) for matches in site_index.values())
            print(f"      ✅ {total_historical} matchs historiques chargés")

            return historical_index

        except Exception as e:
            print(f"      ❌ Erreur chargement historique : {e}")
            traceback.print_exc()
            return historical_index

    async def build_daily_combinaison_sheets(self):
        """Construire les 4 feuilles DailyCombinaison (VERSION OPTIMISÉE - Parse GetSport + Filtrage date)"""

        if not DAILY_COMBINAISON_ENABLED:
            return

        print(f"\n{'='*70}")
        print(f"📊 CONSTRUCTION DailyCombinaison (VERSION RAPIDE)")
        print(f"{'='*70}")

        self._disable_auto_save = True
        try:
            # 1. Récupérer tous les matchs du jour
            print(f"   📡 Récupération matchs du jour ({self.current_date})...")
            all_matches_by_site = await self.get_all_sites_matches(self.current_date)

            # ✅ AJOUT : Date de référence pour filtrage strict
            today = now_mauritius().date()

            # Fusionner par external_id avec filtrage par date
            all_matches = {}
            filtered_out_count = 0  # Compteur pour les matchs filtrés

            for site_key, matches in all_matches_by_site.items():
                for external_id, match_info in matches.items():
                    # ✅ BUG FIX #4: Check if match is within 24 hours (not just today)
                    start_time_str = match_info.get("start_time", "")

                    try:
                        # Parser la date du match
                        match_time_str = start_time_str.replace(',', '').strip()
                        current_year = now_mauritius().year
                        if str(current_year) not in match_time_str:
                            match_time_str = f"{match_time_str} {current_year}"

                        match_dt = date_parser.parse(match_time_str)
                        if match_dt.tzinfo is None:
                            match_dt = match_dt.replace(tzinfo=MAURITIUS_TZ)

                        # ✅ BUG FIX #4: Use 24-hour window (0-24 hours in the future)
                        time_diff_hours = (match_dt - now_mauritius()).total_seconds() / 3600
                        if not (0 <= time_diff_hours <= 24):
                            filtered_out_count += 1
                            continue

                    except Exception:
                        # En cas d'erreur de parsing, on skip le match par sécurité
                        filtered_out_count += 1
                        continue

                    # Ajouter le match validé
                    if external_id not in all_matches:
                        all_matches[external_id] = {}
                    all_matches[external_id][site_key] = match_info

                    if external_id not in self.matches_info_archive:
                        self.matches_info_archive[external_id] = {}
                    self.matches_info_archive[external_id][site_key] = match_info

            total_matches = len(all_matches)
            print(f"   ✅ {total_matches} matchs récupérés (du {self.current_date})")

            if filtered_out_count > 0:
                print(f"   🔍 {filtered_out_count} match(s) filtré(s) (autre date)")

            if total_matches == 0:
                print(f"   ⚠️  Aucun match à analyser")
                return

            # 2. Parser matchData GetSport
            print(f"   🔍 Extraction cotes 1X2 depuis GetSport...")
            odds_1x2_by_match = {}
            success_count = 0

            for site_key in SITES. keys():
                site_name = SITES[site_key]["name"]

                page1_data = await self.get_sport_page(site_key, self.current_date, 1, inclusive=1)

                if not page1_data:
                    print(f"      ⚠️  {site_name}: GetSport échoué")
                    continue

                total_pages = page1_data. get("totalPages", 1)
                all_pages_data = [page1_data]

                if total_pages > 1:
                    tasks = [
                        self.get_sport_page(
                            site_key, self. current_date, page_no, inclusive=0)
                        for page_no in range(2, total_pages + 1)
                    ]
                    other_pages = await asyncio.gather(*tasks, return_exceptions=True)

                    for page_data in other_pages:
                        if isinstance(page_data, Exception) or not page_data:
                            continue
                        all_pages_data.append(page_data)

                site_success = 0
                for page_data in all_pages_data:
                    match_data_str = page_data.get("matchData", "")
                    if not match_data_str:
                        continue

                    for match_str in match_data_str.split("|"):
                        if not match_str. strip():
                            continue

                        try:
                            parts = match_str.split(";")
                            if len(parts) < 29:
                                continue

                            external_id_str = parts[28]
                            if not external_id_str or not external_id_str.isdigit():
                                continue

                            external_id = int(external_id_str)
                            if external_id == 0:
                                continue

                            # ✅ AJOUT : Vérifier que l'external_id est dans les matchs validés
                            if external_id not in all_matches:
                                continue

                            odds_1x2 = self._extract_1x2_from_getsport_match(
                                match_str)

                            if odds_1x2:
                                if external_id not in odds_1x2_by_match:
                                    odds_1x2_by_match[external_id] = {}
                                odds_1x2_by_match[external_id][site_key] = odds_1x2

                                # ✅ AJOUT : Mettre à jour le cache combo immédiatement
                                if external_id not in self.daily_combinaison_cache:
                                    self.daily_combinaison_cache[external_id] = {}
                                self.daily_combinaison_cache[external_id][site_key] = odds_1x2

                                site_success += 1
                                success_count += 1

                        except Exception:
                            continue

                print(f"      ✅ {site_name:15s}: {site_success} cotes extraites")

            matches_with_odds = len([m for m in odds_1x2_by_match.values() if m])
            print(
                f"   ✅ {matches_with_odds} matchs avec cotes valides ({success_count} extractions)")

            if matches_with_odds == 0:
                print(f"   ⚠️  Aucune cote récupérée - abandon")
                return

            # 3. Construire index (matchs du jour SEULEMENT)
            print(f"   🏗️ Construction index combinaisons (matchs du jour)...")
            combinations_index_today = self._build_combinations_index(
                odds_1x2_by_match)

            total_combinations_today = 0

            for site_index in combinations_index_today. values():
                for matches_list in site_index.values():
                    total_combinations_today += len(matches_list)

            print(
                f"   ✅ Index du jour construit ({total_combinations_today} entrées)")

            # 4. ✅ NOUVEAU : Charger historique depuis Google Sheets
            historical_index = await self._load_historical_odds_from_gsheets()

            # 5. ✅ FUSIONNER : Index du jour + Historique
            print(f"   🔗 Fusion index du jour + historique...")
            combined_index = {site_key: {} for site_key in SITES.keys()}

            for site_key in self.SITE_ORDER:
                # Copier matchs du jour
                for odds_key, matches in combinations_index_today[site_key].items():
                    combined_index[site_key][odds_key] = matches. copy()

                # Ajouter matchs historiques
                for odds_key, historical_matches in historical_index[site_key]. items():
                    if odds_key not in combined_index[site_key]:
                        combined_index[site_key][odds_key] = []

                    # Ajouter avec format compatible
                    for h_match in historical_matches:
                        combined_index[site_key][odds_key].append({
                            "external_id": 0,
                            "match_name": h_match["match_name"],
                            "competition": h_match["competition"],
                            "start_time": f"{h_match['date']} {h_match['heure_match']}",
                            "market_count": 0,
                            "is_historical": True
                        })

            total_combined = sum(len(matches) for site_index in combined_index.values(
            ) for matches in site_index. values())
            print(
                f"   ✅ Index combiné : {total_combined} entrées (jour + historique)")

            # 6.  Créer les 4 feuilles avec index combiné
            print(f"   📤 Envoi vers Google Sheets...")
            await self._create_separate_sheets(odds_1x2_by_match, combined_index)

        finally:
            self._disable_auto_save = False
            self.save_cache_to_disk(force=True)


    def _get_active_matches_for_daily_combo(self):
        """
        Retourne la liste des external_id des matchs actifs éligibles à la détection de combination.
        (Matchs non terminés, à venir, dans la fenêtre temporelle, présents dans l'archive.)
        """
        active = []
        now = now_mauritius()
        for external_id, match_info_dict in self.matches_info_archive.items():
            # ✅ BUG FIX #5: Skip completed matches to prevent memory leak
            if external_id in self.completed_matches:
                continue
            
            # Récupère la première info du match
            if not match_info_dict:
                continue

            first_match = list(match_info_dict.values())[0]
            start_time = first_match.get("start_time")
            if not start_time:
                continue
            time_left = self._get_time_until_match(start_time)
            # Par défaut utilise la fenêtre de 6h
            if time_left is not None and 0 <= time_left <= DAILY_COMBINAISON_MAX_TIME_BEFORE_MATCH:
                active.append(external_id)

        return active

    async def update_daily_combinaison_sheets(self):
        """Mise à jour complète : Reconstruit TOUTES les lignes combos pour tous les matchs actifs."""
        time_since_last_check = (
            now_mauritius() - self.last_result_check).total_seconds() / 60

        self._disable_auto_save = True
        try:
            if time_since_last_check >= 60:  # 60 minutes
                if self.gsheets.api_call_count < 45:
                    await self.update_match_results_in_sheets()
                    self.last_result_check = now_mauritius()
                else:
                    print(f"\n   ⚠️ Quota élevé → vérification résultats reportée")

            if not DAILY_COMBINAISON_ENABLED:
                return

            if not self.daily_combinaison_cache:
                return  # Pas encore construit

            print(f"\n🔄 Mise à jour DailyCombinaison...")

            # 1. Récupérer TOUS les matchs actifs (< 6h, non commencés)
            active_matches = self._get_active_matches_for_daily_combo()

            if not active_matches:
                print(f"   ⚠️  Aucun match actif à vérifier")
                return

            print(
                f"   🔍 Reconstruction combinaisons pour {len(active_matches)} matchs actifs...")

            # 2. POLLING GetSport pour tout remettre à jour dans le cache RAM
            current_odds_by_site = {}  # {site_key: {external_id: "1.20/3.00/4.00"}}

            for site_key in self.SITE_ORDER:
                current_odds_by_site[site_key] = {}
                page1_data = await self.get_sport_page(site_key, self.current_date, 1, inclusive=1)

                if not page1_data:
                    continue

                total_pages = page1_data.get("totalPages", 1)
                all_pages_data = [page1_data]

                if total_pages > 1:
                    tasks = [
                        self.get_sport_page(
                            site_key, self.current_date, page_no, inclusive=0)
                        for page_no in range(2, min(total_pages + 1, 10))
                    ]
                    other_pages = await asyncio.gather(*tasks, return_exceptions=True)

                    for page_data in other_pages:
                        if isinstance(page_data, Exception) or not page_data:
                            continue
                        all_pages_data.append(page_data)

                # Parser matchData
                for page_data in all_pages_data:
                    match_data_str = page_data.get("matchData", "")
                    if not match_data_str:
                        continue

                    for match_str in match_data_str.split("|"):
                        if not match_str.strip():
                            continue

                        try:
                            parts = match_str.split(";")
                            if len(parts) < 29:
                                continue

                            external_id_str = parts[28]
                            if not external_id_str or not external_id_str.isdigit():
                                continue

                            external_id = int(external_id_str)
                            if external_id == 0 or external_id not in active_matches:
                                continue

                            odds_1x2 = self._extract_1x2_from_getsport_match(
                                match_str)

                            if odds_1x2:
                                current_odds_by_site[site_key][external_id] = odds_1x2
                                # MAJ cache RAM :
                                if external_id not in self.daily_combinaison_cache:
                                    self.daily_combinaison_cache[external_id] = {}
                                self.daily_combinaison_cache[external_id][site_key] = odds_1x2
                        except Exception:
                            continue

            # 3. Build index combos du jour (RAM)
            new_combinations_index_today = self._build_combinations_index(
                self.daily_combinaison_cache)

            # 4. Charger historique (Sheet 1X2_FullTime) et fusionner
            print(f"   🔗 Fusion RAM + historique...")

            historical_index = await self._load_historical_odds_from_gsheets()
            new_combinations_index = {site_key: {} for site_key in SITES.keys()}

            for site_key in self.SITE_ORDER:
                # Ajouter les matchs du jour
                for odds_key, matches in new_combinations_index_today[site_key].items():
                    new_combinations_index[site_key][odds_key] = matches.copy()

                # Ajouter les historiques
                for odds_key, historical_matches in historical_index[site_key].items():
                    if odds_key not in new_combinations_index[site_key]:
                        new_combinations_index[site_key][odds_key] = []
                    for h_match in historical_matches:
                        new_combinations_index[site_key][odds_key].append({
                            "external_id": 0,
                            "match_name": h_match["match_name"],
                            "competition": h_match["competition"],
                            "start_time": f"{h_match['date']} {h_match['heure_match']}",
                            "market_count": 0,
                            "is_historical": True
                        })

            self.combinations_index_cache = new_combinations_index
            self.combinations_index_cache["last_rebuild"] = now_mauritius()

            # 5. Build la liste de changements : ici on FORCE la regénération de TOUTES les lignes combos !
            # changed_matches = {external_id: {...}} -- on met tous les actifs
            # (le contenu n'a pas d'importance dans ta fonction downstream, c'est la clé qui compte)
            changed_matches = {external_id: {"ALL": True}
                            for external_id in active_matches}

            # 6. MAJ Sheets en reconstruisant toutes les lignes combos
            print(f"   📝 Reconstruction complète des feuilles DailyCombinaison...")
            await self._update_separate_sheets_incremental(changed_matches, new_combinations_index)
            print(f"   ✅ Mise à jour terminée (rebuild full combos) !")
        finally:
            self._disable_auto_save = False
            self.save_cache_to_disk(force=True)
        
    async def _update_summary_sheet_incremental(self, changed_matches: dict, combinations_index: dict):
        """Mettre à jour UNIQUEMENT les lignes modifiées dans DailyCombinaison"""

        try:
            worksheet = self.gsheets.get_or_create_worksheet(
                "DailyCombinaison")

            # Récupérer toutes les données actuelles
            def get_all():
                return worksheet.get_all_values()

            all_values = self.gsheets._execute_with_retry(get_all)

            if not all_values or len(all_values) < 2:
                print(f"      ⚠️  Feuille vide ou corrompue → recréation complète")
                await self._create_summary_sheet(self.daily_combinaison_cache, combinations_index)
                return

            # Header en ligne 1
            header = all_values[0]

            # Trouver index des colonnes
            col_external_id = header.index(
                "External ID") if "External ID" in header else 1

            updates_to_make = []

            # Pour chaque match modifié
            for external_id, sites_changes in changed_matches.items():
                # Trouver la ligne correspondante
                row_index = None
                # Start à 2 (ligne 1 = header)
                for i, row in enumerate(all_values[1:], start=2):
                    if len(row) > col_external_id:
                        try:
                            if int(row[col_external_id]) == external_id:
                                row_index = i
                                break
                        except:
                            continue

                if not row_index:
                    print(
                        f"      ⚠️  Match {external_id} non trouvé dans la feuille")
                    continue

                # Recalculer la ligne complète
                match_info = self.matches_info_archive.get(external_id, {})
                if not match_info:
                    continue

                first_info = list(match_info.values())[0]

                new_row = {
                    "Date": now_mauritius_str("%Y-%m-%d"),
                    "External ID": external_id,
                    "Match Principal": first_info.get("match_name", ""),
                    "Heure Match": first_info.get("start_time", ""),
                    "Compétition": first_info.get("competition_name", "")
                }

                # Pour chaque site, recalculer cotes + nb combos
                for site_key in self.SITE_ORDER:
                    site_name = SITES[site_key]["name"]

                    odds_string = self.daily_combinaison_cache.get(
                        external_id, {}).get(site_key, "")

                    if odds_string:
                        odds_key = odds_string.replace(" ", "")

                        new_row[f"{site_name} Cotes"] = odds_string

                        # Compter nouvelles combinaisons
                        all_matches = combinations_index[site_key].get(
                            odds_key, [])
                        matching_count = len(
                            [m for m in all_matches if m["external_id"] != external_id])

                        if matching_count > 0:
                            new_row[f"{site_name} Nb"] = f"🔥 {matching_count} matchs"
                        else:
                            new_row[f"{site_name} Nb"] = "Aucune"
                    else:
                        new_row[f"{site_name} Cotes"] = "Non disponible"
                        new_row[f"{site_name} Nb"] = ""

                # Préparer update
                row_values = [new_row.get(col, "") for col in header]

                updates_to_make.append({
                    "range": f"A{row_index}",
                    "values": [row_values]
                })

            # Exécuter tous les updates (batch)
            if updates_to_make:
                print(
                    f"      📊 Mise à jour de {len(updates_to_make)} ligne(s)...")

                for update in updates_to_make:
                    def do_update():
                        return worksheet.update(values=update["values"], range_name=update["range"])

                    self.gsheets._execute_with_retry(do_update)
                    await asyncio.sleep(1)  # Pause entre updates

                print(f"      ✅ Feuille DailyCombinaison mise à jour")
            else:
                print(f"      ⚠️  Aucune ligne à mettre à jour")

        except Exception as e:
            print(f"      ❌ Erreur mise à jour résumé : {e}")
            traceback.print_exc()

    async def clear_daily_combinaison_sheets(self):
        """Efface le contenu des 4 feuilles DailyCombinaison dans Google Sheets"""
        print("🧹 Effacement des feuilles DailyCombinaison...")
        for site_key in self.SITE_ORDER:
            sheet_name = f"DailyCombinaison_{SITES[site_key]['name']}"
            worksheet = self.gsheets.get_or_create_worksheet(sheet_name)
            if worksheet:
                try:
                    worksheet.clear()
                    print(f"   ✅ {sheet_name} effacée")
                except Exception as e:
                    print(f"   ❌ Erreur effacement {sheet_name}: {e}")
            else:
                print(f"   ⚠️ Feuille {sheet_name} introuvable")

    async def run_tracking(self, sport="Soccer", interval_seconds=120):
        print("=" * 70)
        print("🎯 MULTI-SITES ODDS TRACKER - VERSION FINALE")
        print("=" * 70)
        print(f"Sites: {', '.join([s['name'] for s in SITES.values()])}")
        print(f"Google Sheets: {GOOGLE_SHEETS_CONFIG['sheet_name']}")
        print(f"Excel local: {self.local_cumulative_excel.name}")
        print(f"📦 Batch size: {self.finalization_queue.batch_size} matchs")
        print(
            f"⏱️  Intervalle batch: {self.finalization_queue.min_interval_seconds}s")
        print(f"📅 Date: {self.current_date}")
        print(f"🔄 Capture dynamique: Toutes les 2 min (matchs < 60 min)")
        print(f"📸 Capture avant disparition: Immédiate")
        print(f"🆔 Identifiant: externalId (index 28)")
        print(f"Fuseau: Maurice (UTC+4)")
        print(f"Utilisateur: antema102")
        print(f"✅ V2.5: Persistence + anti double finalisation + DailyCombinaison")
        print("=" * 70)
        print()

        self.last_result_check = now_mauritius()

        if len(self.completed_matches) > 0 or self.local_cumulative_excel.exists():
            print(f"⚠️  Fichier Excel détecté")

            resync_choice = input(
                "\n🔄 Resynchroniser Google Sheets depuis Excel ? (O/n) : ").strip().lower()

            if resync_choice != 'n':
                print("\n🚀 Lancement resynchronisation...")
                success = await self.resync_from_excel_to_gsheets()

                if success:
                    print("✅ Resynchronisation terminée avec succès !")
                    input("\n⏸️  Appuyez sur ENTRÉE pour continuer le tracking...")
                else:
                    print("⚠️  Resynchronisation avec erreurs")
                    choice = input(
                        "\n❓ Continuer quand même ? (O/n) : ").strip().lower()
                    if choice == 'n':
                        return

        queue_task = asyncio.create_task(self.process_finalization_queue())

        try:
            while True:
                self.iteration += 1

                print(f"\n{'='*70}")
                print(f"🔄 ITERATION #{self.iteration} - {now_mauritius_str()}")
                print(f"{'='*70}")

                self._check_date_change()

                # Poll for odds changes
                if self.iteration % self.DAILY_COMBO_UPDATE_INTERVAL == 0:
                    await self.poll_all_sites_and_detect_odds_changes()

                should_full_getsport = (self.iteration %
                                        self.FULL_GETSPORT_INTERVAL == 1)

                if should_full_getsport:
                    print(f"\n📡 GetSport COMPLET (pagination parallèle)")
                    matches_by_site = await self.get_all_sites_matches(self.current_date)

                    for site_key, matches in matches_by_site.items():
                        for external_id, match_info in matches.items():
                            if external_id not in self.matches_info_archive:
                                self.matches_info_archive[external_id] = {}
                            self.matches_info_archive[external_id][site_key] = match_info

                            if external_id not in self.matches_by_external_id:
                                self.matches_by_external_id[external_id] = {}
                            self.matches_by_external_id[external_id][site_key] = match_info

                    await self.detect_early_closures_and_reopenings(matches_by_site)

                    self.last_getsport_full = now_mauritius()

                    self.gsheets.invalidate_cache()

                    # NOUVEAU : Construction DailyCombinaison (première itération)
                    if self.iteration == 1 and DAILY_COMBINAISON_ENABLED:
                        await self.build_daily_combinaison_sheets()

                else:
                    print(f"\n🔍 Vérification LÉGÈRE (matchs proches)")

                await self.verify_close_matches_availability()

                # Retry matches without odds
                if self.iteration % self.RETRY_WITHOUT_ODDS_INTERVAL == 0:
                    await self.retry_matches_without_odds()

                # Update DailyCombinaison sheets
                if self.iteration > 1 and self.iteration % self.DAILY_COMBO_UPDATE_INTERVAL == 0 and DAILY_COMBINAISON_ENABLED:
                    await self.update_daily_combinaison_sheets()

                await self.check_matches_for_finalization()

                print(
                    f"\n   📊 Matchs suivis : {len(self.matches_info_archive)}")
                print(f"   ✅ Matchs complétés : {len(self.completed_matches)}")
                print(f"   📦 Queue : {len(self.finalization_queue)} matchs")
                print(
                    f"   💾 Cotes en cache : {sum(len(sites) for sites in self.captured_odds.values())} sites")

                # Health report
                if self.iteration % self.HEALTH_REPORT_INTERVAL == 0:
                    self.api_health.print_report()

                # Reset stats si nécessaire
                self.api_health.reset_if_needed()

                # Cleanup old matches to prevent memory leak
                if self.iteration % self.CLEANUP_INTERVAL == 0:
                    self._cleanup_old_matches()

                # Sauvegarde périodique de l'état
                self.save_cache_to_disk()

                print(
                    f"\n   ⏳ Prochaine itération dans {interval_seconds}s...")
                await asyncio.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n\n⚠️  Arrêt manuel")
            queue_task.cancel()

        finally:
            # Sauvegarde finale
            self.save_cache_to_disk(force=True)
            print("\n✅ Arrêt du script")
            print(f"📊 Queue finale : {len(self.finalization_queue)} matchs")


async def main():
    async with MultiSitesOddsTrackerFinal(output_dir="multi_sites_odds") as tracker:
        await tracker.run_tracking(
            sport="Soccer",
            interval_seconds=120
        )


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 TRACKER MULTI-SITES - VERSION FINALE")
    print("=" * 70)
    print(f"📅 Date: {now_mauritius_str('%Y-%m-%d')}")
    print(f"🕐 Heure Maurice: {now_mauritius_str()}")
    print(f"👤 Utilisateur: antema102")
    print(f"✅ V2.4: Persistence + anti double finalisation")
    print("=" * 70)
    print()
    asyncio.run(main())
