#main_prase.py
from parser import run_parser
from telegram_utils import send_telegram_message

if __name__ == "__main__":
    result = run_parser()

    rights_added = result["rights_added"]
    bond_added = result["bond_added"]
    total_added = result["total_added"]
    error_count = result["error_count"]

    if total_added > 0 or error_count > 0:
        send_telegram_message(
            "[KIND Pipeline 알림]\n"
            f"유상증자 추가: {rights_added}\n"
            f"주식연계채권 추가: {bond_added}\n"
            f"총 신규 반영: {total_added}\n"
            f"에러 건수: {error_count}"
        )
