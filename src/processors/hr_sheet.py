import logging
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from src.config import settings
from src.logging_config import setup_logging
from src.processors.base import BaseProcessor
from src.utils.gdrive_utils import (
    get_gdrive_service,
    upload_file_to_gdrive,
    delete_files_in_folder,
)
from src.utils.gsheets_utils import get_gsheets_service, read_sheet_values

logger = logging.getLogger(__name__)


def normalize_header(header: str) -> str:
    return header.strip().lower()


def parse_hr_rows(values: List[List[str]]) -> List[Dict[str, Any]]:
    if not values:
        return []
    headers = [normalize_header(h) for h in values[0]]
    data_rows = []
    for row in values[1:]:
        entry: Dict[str, Any] = {}
        for idx, col in enumerate(row):
            if idx >= len(headers):
                continue
            key = headers[idx]
            entry[key] = col.strip()
        data_rows.append(entry)
    return data_rows


def extract_kv_lines(entries: List[Dict[str, Any]]) -> List[Tuple[str, Optional[str]]]:
    lines: List[Tuple[str, Optional[str]]] = []
    for e in entries:
        name = e.get("имя", "").strip() or e.get("name", "").strip()
        if not name:
            continue
        direction = e.get("направление", "").strip()
        teamlead = e.get("тимлид", "").strip()
        current_position = (
                e.get("текущая позиция", "").strip() or e.get("позиция", "").strip()
        )
        start_date = (
                             e.get("дата начала работы", "").strip() or e.get("начало ис", "").strip()
                     ) or None
        probation_end = e.get("конец ис", "").strip()
        probation_passed = e.get("ис пройден", "").strip()
        termination_status = e.get("увольнение", "").strip()
        termination_reason = e.get("причина увольнения", "").strip()
        termination_date = (
                                   e.get("дата", "").strip() or e.get("дата расторжения договора", "").strip()
                           ) or None

        parts: List[str] = [f"person: {name}"]
        if current_position:
            parts.append(f"current_position: {current_position}")
        if direction:
            parts.append(f"direction: {direction}")
        if teamlead:
            parts.append(f"team_lead: {teamlead}")
        if start_date:
            parts.append(f"start_date: {start_date}")
        if probation_end:
            passed = probation_passed or "unknown"
            parts.append(f"probation_end: {probation_end}; probation_passed: {passed}")
        if termination_status:
            parts.append(f"termination_status: {termination_status}")
        if termination_date:
            parts.append(f"termination_date: {termination_date}")
        if termination_reason:
            parts.append(f"termination_reason: {termination_reason}")

        primary_date = start_date or termination_date
        lines.append(("; ".join(parts), primary_date))

    return lines


def process_hr_sheet() -> None:
    """Entrypoint wrapper for class-based HR processing."""
    setup_logging()
    processor = HRSheetProcessor(logger=logging.getLogger(__name__))
    processor.run()


class HRSheetProcessor(BaseProcessor):
    """Process HR Google Sheet into knowledge text."""

    def run(self) -> None:
        if (
                not settings.google_sheets_hr_spreadsheet_id
                or not settings.google_drive_hr_processed_dir_id
        ):
            logger.warning(
                "HR sheet settings are not configured; skipping HR processor."
            )
            return

        gsheets = get_gsheets_service()
        if not gsheets:
            logger.error("Could not get Google Sheets service. Aborting HR processing.")
            return

        gdrive = get_gdrive_service()
        if not gdrive:
            logger.error("Could not get Google Drive service. Aborting HR processing.")
            return

        logger.info(
            f"Clearing processed folder ID: {settings.google_drive_hr_processed_dir_id}..."
        )
        delete_files_in_folder(gdrive, settings.google_drive_hr_processed_dir_id)
        logger.info("Processed folder cleared.")

        sheet_name = settings.google_sheets_hr_sheet_name or "Sheet1"
        range_a1 = f"{sheet_name}!{settings.google_sheets_hr_range}"

        logger.info(
            f"Reading HR sheet '{sheet_name}' (range {settings.google_sheets_hr_range}) from spreadsheet {settings.google_sheets_hr_spreadsheet_id}"
        )

        values = read_sheet_values(
            gsheets, settings.google_sheets_hr_spreadsheet_id, range_a1
        )
        if not values:
            logger.info("No data returned from HR sheet.")
            return

        if len(values) <= 1:
            logger.info(
                "HR sheet does not have enough rows after skipping the first row."
            )
            return
        entries = parse_hr_rows(values[1:])
        kv_lines = extract_kv_lines(entries)

        def parse_date(date_str: Optional[str]) -> Optional[str]:
            if not date_str:
                return None
            s = date_str.strip()
            # Try YYYY-MM-DD
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
            if m:
                return s
            # Try DD.MM.YYYY
            m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
            if m:
                return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            return None

        dated_items: List[Tuple[str, Optional[str]]] = [
            (text, parse_date(dt)) for text, dt in kv_lines
        ]
        dated_items.sort(key=lambda x: (x[1] or "9999-12-31", x[0]))

        split_count = settings.hr_split_files_count or 1
        if split_count < 1:
            split_count = 1

        chunks: List[List[Tuple[str, Optional[str]]]] = []
        if split_count == 1:
            chunks = [dated_items]
        else:
            n = len(dated_items)
            if n == 0:
                chunks = []
            else:
                per = max(1, n // split_count)
                for i in range(0, n, per):
                    chunks.append(dated_items[i: i + per])

        if not chunks:
            logger.info("No HR entries to write.")
            return

        for idx, chunk in enumerate(chunks, start=1):
            if not chunk:
                continue
            first_dt = next((d for _, d in chunk if d), None) or "unknown"
            last_dt = next((d for _, d in reversed(chunk) if d), None) or "unknown"

            metadata = (
                "---\n"
                "source: HR\n"
                "category: HR lifecycle\n"
                "tz: Europe/Moscow\n"
                "body_format: kv-lines\n"
                "---\n\n"
            )

            file_suffix = (
                f"_{first_dt}_to_{last_dt}"
                if first_dt != "unknown" or last_dt != "unknown"
                else ""
            )
            file_name = (
                f"hr_people_knowledge_part_{idx}{file_suffix}.txt"
                if len(chunks) > 1
                else "hr_people_knowledge.txt"
            )
            output_path: Path = settings.temp_dir / file_name
            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(metadata)
                    f.write("\n".join(text for text, _ in chunk))
                logger.info(f"Generated HR knowledge file: {output_path}")

                logger.info(
                    f"Uploading {output_path.name} to Google Drive as a Google Doc..."
                )
                upload_file_to_gdrive(
                    gdrive,
                    output_path,
                    settings.google_drive_hr_processed_dir_id,
                    as_gdoc=True,
                )
            except OSError as e:
                logger.error(f"Error writing HR knowledge file {output_path}: {e}")


if __name__ == "__main__":
    process_hr_sheet()
