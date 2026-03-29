import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from html import unescape

# ==========================================
# 基礎設定
# ==========================================
DATA_DIR = "./daily_etf_data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}

# 輔助函式：自動在 JSON 中尋找股票清單 (針對群益)
def find_stock_list(obj):
    if isinstance(obj, list):
        if len(obj) > 0 and isinstance(obj[0], dict) and ("stocNo" in obj[0] or "stocName" in obj[0]):
            return obj
        for item in obj:
            result = find_stock_list(item)
            if result: return result
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                result = find_stock_list(v)
                if result: return result
    return None

# ==========================================
# 1. 抓取 00981A (統一) - 僅支援最新資料
# ==========================================
def fetch_00981A_latest():
    url = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200: return None
        
        soup = BeautifulSoup(r.text, "html.parser")
        tag = soup.find("div", {"id": "DataAsset"})
        if not tag: return None

        raw = unescape(tag.get("data-content", ""))
        asset_list = json.loads(raw)

        stocks = None
        for item in asset_list:
            if item.get("AssetCode") == "ST" and item.get("Details"):
                stocks = item["Details"]
                break
        if not stocks: return None

        rows = []
        data_date = ""
        for s in stocks:
            # 抓取資料本身的日期 (格式如 2026/03/27)
            raw_date = s.get("TranDate", "")[:10]
            data_date = raw_date.replace("/", "-") if raw_date else datetime.today().strftime("%Y-%m-%d")
            
            rows.append({
                "日期": data_date,
                "ETF代號": "00981A",
                "股票代號": s.get("DetailCode", "").strip(),
                "股票名稱": s.get("DetailName", "").strip(),
                "股數": float(s.get("Share", 0)),
                "持股比例(%)": float(s.get("NavRate", 0)),
            })
            
        if rows:
            print(f"  ✅ [00981A] 成功取得 {data_date} 最新資料 (共 {len(rows)} 檔)")
            return pd.DataFrame(rows), data_date
        return None, None
    except Exception as e:
        print(f"  ❌ [00981A] 發生錯誤: {e}")
        return None, None

# ==========================================
# 2. 抓取 00980A (野村)
# ==========================================
def fetch_00980A(target_date: str):
    api_url = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"
    payload = {"FundID": "00980A", "SearchDate": target_date}
    try:
        r = requests.post(api_url, json=payload, headers=HEADERS, timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        fund_data = data.get('Entries', {}).get('Data', {})
        if not fund_data: return None 
            
        tables = fund_data.get('Table', [])
        rows = []
        for table in tables:
            if table.get('TableTitle') == '股票':
                for r in table['Rows']:
                    try:
                        rows.append({
                            "日期": target_date, "ETF代號": "00980A",
                            "股票代號": str(r[0]).strip(), "股票名稱": str(r[1]).strip(),
                            "股數": float(r[2]) if str(r[2]).replace('.', '').isdigit() else 0,
                            "持股比例(%)": float(r[3]) if str(r[3]).replace('.', '').isdigit() else 0,
                        })
                    except: continue
                break
        if rows:
            print(f"  ✅ [00980A] 成功取得 {target_date} 資料 (共 {len(rows)} 檔)")
            return pd.DataFrame(rows)
        return None
    except Exception as e:
        print(f"  ❌ [00980A] 發生錯誤: {e}")
        return None

# ==========================================
# 3. 抓取 00982A (群益)
# ==========================================
def fetch_00982A(target_date: str):
    api_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
    custom_headers = {
        **HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.capitalfund.com.tw",
        "Referer": "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    }
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        utc_dt = dt - timedelta(days=1)
        formatted_date = utc_dt.strftime("%Y-%m-%dT16:00:00.000Z")
    except: return None

    payload = {"fundId": "399", "date": formatted_date}
    try:
        r = requests.post(api_url, json=payload, headers=custom_headers, timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        if isinstance(data, dict) and data.get("code") not in [200, None]: return None
            
        stock_list = find_stock_list(data)
        if not stock_list: return None
            
        rows = []
        for s in stock_list:
            stoc_no = str(s.get("stocNo", "")).strip()
            if not stoc_no: continue
            rows.append({
                "日期": target_date, "ETF代號": "00982A",
                "股票代號": stoc_no, "股票名稱": str(s.get("stocName", "")).strip(),
                "股數": float(s.get("share", 0)),
                "持股比例(%)": float(s.get("weightRound", s.get("weight", 0))),
            })
        if rows:
            print(f"  ✅ [00982A] 成功取得 {target_date} 資料 (共 {len(rows)} 檔)")
            return pd.DataFrame(rows)
        return None
    except Exception as e:
        print(f"  ❌ [00982A] 發生錯誤: {e}")
        return None

# ==========================================
# 主流程：自動抓取並合併存檔 (破除假日魔咒版)
# ==========================================
def main():
    print("========================================")
    print("      主動 ETF 每日例行追蹤工具")
    print("========================================")
    
    # 產生最近 5 天內的「平日」日期清單 (排除六日)
    check_dates = []
    for i in range(5):
        d = datetime.today() - timedelta(days=i)
        # weekday() 0~4 代表週一到週五，5是週六，6是週日
        if d.weekday() < 5:  
            check_dates.append(d.strftime("%Y-%m-%d"))

    print(f"💡 系統排定的檢查日期順序 (優先找最近交易日): {check_dates}")
    all_data = []

    print("\n🔍 正在抓取 00981A (統一) 最新資料...")
    df_00981a, actual_date_00981a = fetch_00981A_latest()
    if df_00981a is not None:
        all_data.append(df_00981a)

    print("\n🔍 正在抓取 00980A (野村)...")
    for d in check_dates:
        df_00980a = fetch_00980A(d)
        if df_00980a is not None: 
            all_data.append(df_00980a)
            break
        else:
            print(f"  ⚠️ {d} 無資料，嘗試往前找...")

    print("\n🔍 正在抓取 00982A (群益)...")
    for d in check_dates:
        df_00982a = fetch_00982A(d)
        if df_00982a is not None: 
            all_data.append(df_00982a)
            break
        else:
            print(f"  ⚠️ {d} 無資料，嘗試往前找...")

    print("\n========================================")
    if all_data:
        # 決定存檔名稱 (優先使用 00981A 抓到的實際日期，否則用今天)
        save_date = actual_date_00981a if actual_date_00981a else datetime.today().strftime("%Y-%m-%d")
        filename = os.path.join(DATA_DIR, f"ETF三雄_持股日報_{save_date}.csv")
        
        # 合併所有 DataFrame 並存檔
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"🎉 大功告成！三檔資料已合併存檔: {filename} (總計 {len(final_df)} 筆)")
    else:
        print("❌ 糟糕，什麼資料都沒抓到，請檢查網路狀態或 API 是否改版。")

if __name__ == "__main__":
    main()
