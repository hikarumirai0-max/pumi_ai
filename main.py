import os
import re
import json
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# =========================================================
# 1) OpenAI 설정
# =========================================================
# API 키는 코드에 직접 넣지 말고 환경변수 OPENAI_API_KEY 사용
# 예: set OPENAI_API_KEY=sk-xxxx
ai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# 빠르고 저렴한 AI형 운영용 기본값
AI_MODEL = "gpt-5.4-nano"

# AI 요약 답변 켜기/끄기
USE_AI_SUMMARY = False

# =========================================================
# 2) 구글 시트 연결
# =========================================================
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

import streamlit as st

creds_dict = dict(st.secrets["gcp_service_account"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

gs_client = gspread.authorize(creds)

# 꼭 네 시트 URL로 바꿔
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NoBl8BXmSU8KEjuap2FAspD9cx2ANkNzgM1FRKmxn-4/edit?gid=0#gid=0"

sheet = gs_client.open_by_key("docs.google.com/spreadsheets/d/1rl3HMS5GdLBXf57-MDBgF5y0DXxoBsZFvtiZE0jwaqY").sheet1


def load_data():
    return sheet.get_all_records()


def get_headers():
    return sheet.row_values(1)


data = load_data()
HEADERS = get_headers()

# =========================================================
# 3) 기본 열 정의
# =========================================================
COL_COMPANY = "거래처명"
COL_SITE = " 부서명/현장명"
COL_MANAGER = "관리\n담당자"
COL_PHONE = "일반전화"
COL_MODEL = "모델명"
COL_ITEM = "품목"
COL_ADDRESS = "주소(실납품주소,도로명주소)"
COL_SERIAL = "시리얼번호(기번)"
COL_ASSET = "자산번호"

SEARCH_PRIORITY_COLUMNS = [
    COL_COMPANY,
    COL_SITE,
    "업체명",
    COL_MODEL,
    COL_ITEM,
    COL_SERIAL,
    COL_ASSET,
    COL_ADDRESS,
    COL_MANAGER,
]

UPDATABLE_ALIASES = {
    "담당자": COL_MANAGER,
    "관리담당자": COL_MANAGER,
    "관리 담당자": COL_MANAGER,
    "연락처": COL_PHONE,
    "전화번호": COL_PHONE,
    "일반전화": COL_PHONE,
}

# =========================================================
# 4) 유틸
# =========================================================
def normalize_text(text):
    if text is None:
        return ""
    return str(text).replace("\n", " ").replace("\t", " ").strip().lower()


def clean_label(text):
    return str(text).replace("\n", " ").strip()


def clean_company_name(name):
    text = normalize_text(name)
    text = text.replace("주식회사", "")
    text = text.replace("(주)", "")
    text = text.replace("㈜", "")
    text = " ".join(text.split())
    return text.strip()


def value_exists(value):
    t = normalize_text(value)
    return t not in {"", "-", "x", "없음", "none", "nan"}


def is_valid_row(row):
    return any(value_exists(v) for v in row.values())


def is_full_info_request(question):
    q = normalize_text(question)
    signals = [
        "전체 정보", "전체정보", "전부", "모든 정보", "모든정보",
        "전체 보여", "전부 보여", "모두 보여", "전체 알려", "전체 다"
    ]
    return any(s in q for s in signals)


def safe_get(row, key):
    return row.get(key, "")


def unique_keep_order(items):
    result = []
    seen = set()
    for item in items:
        if item and item not in seen:
            result.append(item)
            seen.add(item)
    return result


def remove_duplicates(rows):
    seen = set()
    result = []

    for row in rows:
        key = (
            normalize_text(row.get(COL_COMPANY, "")),
            normalize_text(row.get(COL_SITE, "")),
            normalize_text(row.get(COL_MODEL, "")),
            normalize_text(row.get(COL_ITEM, "")),
        )
        if key not in seen:
            seen.add(key)
            result.append(row)

    return result


# =========================================================
# 5) 헤더 동적 매칭
# =========================================================
HEADER_ALIAS_CANDIDATES = {
    "거래처": COL_COMPANY,
    "업체": COL_COMPANY,
    "업체명": COL_COMPANY,
    "거래처명": COL_COMPANY,
    "현장": COL_SITE,
    "현장명": COL_SITE,
    "부서": COL_SITE,
    "부서명": COL_SITE,
    "주소": COL_ADDRESS,
    "관리담당자": COL_MANAGER,
    "관리 담당자": COL_MANAGER,
    "담당자": COL_MANAGER,
    "연락처": COL_PHONE,
    "전화번호": COL_PHONE,
    "일반전화": COL_PHONE,
    "시리얼": COL_SERIAL,
    "시리얼번호": COL_SERIAL,
    "기번": COL_SERIAL,
    "자산번호": COL_ASSET,
    "모델": COL_MODEL,
    "모델명": COL_MODEL,
    "품목": COL_ITEM,
    "기본금액": "기본금액",
    "보증금": "보증금",
    "계약기간": "계약기간",
    "계약년수": "계약년수",
    "종료일": "종료일",
    "남은개월": "남은개월",
    "미수금": "미수금액",
    "미수금액": "미수금액",
    "미수개월": "미수개월수",
    "미수개월수": "미수개월수",
    "미수담당": "미수담당",
    "초과료": "추가조건",
    "추가컬": "추가(컬)",
    "추가흑": "추가(흑)",
    "누적방식": "누적방식\n(월/분/반/년)",
}


def build_header_alias_map(headers):
    result = {}
    normalized_header_map = {normalize_text(h): h for h in headers}

    for alias, target in HEADER_ALIAS_CANDIDATES.items():
        if target in headers:
            result[alias] = target
        else:
            target_norm = normalize_text(target)
            if target_norm in normalized_header_map:
                result[alias] = normalized_header_map[target_norm]

    for h in headers:
        result[normalize_text(h)] = h
        result[clean_label(h)] = h
        result[clean_label(h).lower()] = h

    return result


HEADER_ALIAS_MAP = build_header_alias_map(HEADERS)

# =========================================================
# 6) 규칙 기반 질문 패턴 (fallback)
# =========================================================
QUESTION_PATTERNS = {
    "contract_remaining": {
        "keywords": [
            "계약기간 얼마나 남았", "얼마나 남았", "남은개월", "잔여개월",
            "몇 개월 남았", "언제 끝나", "종료일"
        ],
        "fields": ["남은개월", "종료일", "계약기간", "계약년수"]
    },
    "unpaid": {
        "keywords": [
            "미수금", "미수금액", "못 받은 금액", "미수 얼마", "미수 있나"
        ],
        "fields": ["미수금액", "미수개월수", "미수담당"]
    },
    "basic_amount": {
        "keywords": [
            "기본금액", "기본 금액", "월금액", "월요금", "기본요금", "금액 얼마"
        ],
        "fields": ["기본금액"]
    },
    "overage_fee": {
        "keywords": [
            "초과료", "추가요금", "추가 금액", "초과 사용료", "초과금액", "많이 나오나"
        ],
        "fields": ["추가(컬)", "추가(흑)", "추가조건", "누적방식\n(월/분/반/년)"]
    },
    "contract_years": {
        "keywords": [
            "몇년 계약", "몇 년 계약", "계약년수", "계약 몇년", "몇년으로 되어있어"
        ],
        "fields": ["계약년수", "계약기간", "종료일"]
    },
    "address_contact": {
        "keywords": [
            "주소와 담당자 연락처", "주소랑 담당자 연락처", "주소와 연락처",
            "주소랑 연락처", "주소와 담당자", "주소 알려주고 연락처"
        ],
        "fields": [COL_ADDRESS, COL_MANAGER, COL_PHONE]
    },
    "asset_serial": {
        "keywords": [
            "자산기번", "자산번호", "자산기번과 시리얼", "시리얼 번호",
            "시리얼번호", "기번", "자산번호와 시리얼"
        ],
        "fields": [COL_ASSET, COL_SERIAL]
    },
    "deposit": {
        "keywords": ["보증금", "보증금 얼마", "보증 얼마"],
        "fields": ["보증금"]
    },
    "manager": {
        "keywords": ["관리 담당자", "담당자", "담당자 누구", "관리자 누구"],
        "fields": [COL_MANAGER, COL_PHONE]
    },
    "model": {
        "keywords": ["모델명", "모델", "무슨 모델", "어떤 모델"],
        "fields": [COL_MODEL, COL_ITEM]
    }
}


def detect_question_type(question):
    q = normalize_text(question)
    for qtype, info in QUESTION_PATTERNS.items():
        for keyword in info["keywords"]:
            if normalize_text(keyword) in q:
                return qtype
    return None


def get_target_fields_by_rule(question_type):
    if question_type and question_type in QUESTION_PATTERNS:
        return QUESTION_PATTERNS[question_type]["fields"]
    return []


# =========================================================
# 7) GPT 파서 (Structured Outputs)
# =========================================================
PARSER_SCHEMA = {
    "name": "pumi_query_parser",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "enum": ["query", "update"]
            },
            "entity": {
                "type": "string"
            },
            "fields": {
                "type": "array",
                "items": {"type": "string"}
            },
            "full_info": {
                "type": "boolean"
            },
            "update_field": {
                "type": "string"
            },
            "new_value": {
                "type": "string"
            }
        },
        "required": ["intent", "entity", "fields", "full_info", "update_field", "new_value"],
        "additionalProperties": False
    }
}


def ask_gpt_parser(question, last_entity_text=""):
    system_prompt = f"""
너는 한국어 업무 질문을 구조화하는 파서다.
반드시 JSON 스키마에 맞게만 응답한다.

현재 시트 헤더:
{json.dumps([clean_label(h) for h in HEADERS], ensure_ascii=False)}

규칙:
- 조회면 intent=query
- 수정이면 intent=update
- 전체/전부/모든 정보 요청이면 full_info=true
- 수정 가능 필드는 담당자, 관리담당자, 연락처, 전화번호, 일반전화만
- fields는 최대한 실제 시트 헤더와 가까운 이름으로 추정
- 미수금 질문이면 fields에 미수금액, 미수개월수, 미수담당 포함
- 계약 남은 기간 질문이면 남은개월, 종료일, 계약기간, 계약년수 포함
- 주소와 담당자 연락처 질문이면 주소, 담당자, 연락처 포함
- 자산기번과 시리얼 질문이면 자산번호, 시리얼번호 포함
- 사용자가 대상을 생략했고 이전 검색 대상이 있으면 entity에 그 값을 넣어도 됨
"""

    user_prompt = f"""
이전 검색 대상: {last_entity_text if last_entity_text else "없음"}
사용자 질문: {question}
"""

    try:
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            temperature=0,
            response_format={
                "type": "json_schema",
                "json_schema": PARSER_SCHEMA
            },
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception:
        return None


# =========================================================
# 8) AI 답변 요약
# =========================================================
def build_summary_payload(rows, selected_fields, full_info, updated=False):
    payload = []
    for row in rows[:3]:
        item = {
            "거래처명": safe_get(row, COL_COMPANY),
            "현장명": safe_get(row, COL_SITE),
            "품목": safe_get(row, COL_ITEM),
            "모델명": safe_get(row, COL_MODEL),
        }

        if full_info:
            for h in HEADERS:
                item[clean_label(h)] = safe_get(row, h)
        else:
            for field in selected_fields:
                item[clean_label(field)] = safe_get(row, field)

        payload.append(item)

    return payload


def ask_ai_summary(question, rows, selected_fields, full_info):
    if not USE_AI_SUMMARY:
        return None

    payload = build_summary_payload(rows, selected_fields, full_info)

    prompt = f"""
사용자 질문에 답하듯 한국어로 짧고 자연스럽게 정리해라.
불필요한 장황함 없이 핵심만 말해라.
데이터가 여러 건이면 가장 유력한 결과를 먼저 말하고, 필요한 경우 다른 후보가 있다고 짧게 언급해라.

사용자 질문:
{question}

조회 결과:
{json.dumps(payload, ensure_ascii=False)}
"""

    try:
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "너는 업무용 데이터 요약 도우미다."},
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return None


# =========================================================
# 9) 필드 자동 매핑
# =========================================================
def map_field_name(name):
    if not name:
        return None

    raw = str(name).strip()
    norm = normalize_text(raw)

    if raw in HEADERS:
        return raw

    if norm in HEADER_ALIAS_MAP:
        return HEADER_ALIAS_MAP[norm]

    if raw in HEADER_ALIAS_MAP:
        return HEADER_ALIAS_MAP[raw]

    for h in HEADERS:
        h_norm = normalize_text(h)
        if norm and (norm in h_norm or h_norm in norm):
            return h

    return None


def map_field_list(field_names):
    mapped = []
    for name in field_names or []:
        real = map_field_name(name)
        if real:
            mapped.append(real)
    return unique_keep_order(mapped)


def detect_header_fields_from_question(question):
    q = normalize_text(question)
    found = []

    for h in HEADERS:
        label = clean_label(h)
        if normalize_text(label) in q:
            found.append(h)

    for alias, real_header in HEADER_ALIAS_MAP.items():
        if normalize_text(alias) in q:
            found.append(real_header)

    return unique_keep_order(found)


# =========================================================
# 10) 검색
# =========================================================
def extract_search_terms(question):
    q = normalize_text(question)

    stopwords = {
        "얼마야", "얼마", "뭐야", "뭔가", "뭔지", "알려줘", "보여줘", "찾아줘",
        "조회", "관련", "정보", "좀", "이거", "저거", "어디", "누구", "언제",
        "몇", "남았어", "남았나", "확인", "해줘", "되어있어", "되있어",
        "많이", "나오나", "이", "가", "은", "는", "을", "를", "와", "과",
        "그리고", "그럼", "그", "거기", "거", "여기", "저기", "또", "다시",
        "바꿔줘", "바꿔", "수정해줘", "수정", "변경해줘", "변경", "고쳐줘", "고쳐",
        "로", "으로", "전체", "전부", "모든", "정보", "모두"
    }

    removable_words = set(stopwords)
    for h in HEADERS:
        removable_words.add(normalize_text(clean_label(h)))
    for alias in HEADER_ALIAS_MAP.keys():
        removable_words.add(normalize_text(alias))

    words = re.split(r"[ ,?]+", q)
    results = []

    for w in words:
        w = w.strip()
        if not w:
            continue
        if len(w) < 2:
            continue
        if w in removable_words:
            continue
        results.append(w)

    return results


def row_matches(row, search_terms):
    if not search_terms:
        return True

    search_text = " ".join(
        normalize_text(row.get(col, ""))
        for col in SEARCH_PRIORITY_COLUMNS if col in row
    )

    if not search_text:
        search_text = " ".join(normalize_text(v) for v in row.values())

    return all(term in search_text for term in search_terms)


def score_row(row, target_fields, search_terms):
    score = 0

    company = clean_company_name(row.get(COL_COMPANY, ""))
    site = normalize_text(row.get(COL_SITE, ""))
    address = normalize_text(row.get(COL_ADDRESS, ""))

    for term in search_terms:
        term_clean = clean_company_name(term)
        term_norm = normalize_text(term)

        # 회사명 매칭
        if term_clean == company:
            score += 50
        elif term_clean and term_clean in company:
            score += 30

        # 현장명 매칭
        if term_norm and term_norm in site:
            score += 60

        # 주소 매칭
        if term_norm and term_norm in address:
            score += 80

    # 질문한 필드 값이 실제로 있으면 보너스
    for field in target_fields:
        if field in row and value_exists(row.get(field, "")):
            score += 5

    return score


def find_rows_by_entity(entity):
    if not entity:
        return []

    entity_clean = clean_company_name(entity)
    entity_norm = normalize_text(entity)
    results = []

    for row in data:
        if not is_valid_row(row):
            continue

        # 자산기번 / 시리얼 / 일반 문자열 강제 포함 검색
        row_text = str(row).lower()
        if entity and entity.lower() in row_text:
            results.append(row)
            continue

        company = clean_company_name(row.get(COL_COMPANY, ""))
        site = normalize_text(row.get(COL_SITE, ""))

        # 업체명 매칭
        if entity_clean and (entity_clean in company or company in entity_clean):
            results.append(row)
            continue

        # 주소 매칭
        if entity_norm and entity_norm in site:
            results.append(row)

    return results

def find_rows_by_terms(search_terms):
    return [
        row for row in data
        if is_valid_row(row) and row_matches(row, search_terms)
    ]


def find_best_entity_from_match(row):
    site = str(row.get(COL_SITE, "")).strip()
    company = str(row.get(COL_COMPANY, "")).strip()

    if site and site != "-":
        return site
    if company:
        return company
    return ""


def is_followup_question(question):
    q = normalize_text(question)
    signals = [
        "담당자는", "담당자", "연락처", "전화번호", "보증금은", "기본금액은",
        "미수금은", "계약기간은", "남은개월은", "종료일은", "주소는",
        "시리얼은", "자산번호는", "모델명은", "전체 정보", "전부", "모든 정보",
        "그 업체", "그 현장", "거기", "그거", "그곳"
    ]
    return any(s in q for s in signals)


# =========================================================
# 11) 출력
# =========================================================
def print_row_header(index, row):
    print("-" * 60)
    print(f"[{index}] 거래처명: {safe_get(row, COL_COMPANY)}")
    print(f"[{index}] 현장명: {safe_get(row, COL_SITE)}")
    print(f"[{index}] 품목: {safe_get(row, COL_ITEM)}")
    print(f"[{index}] 모델명: {safe_get(row, COL_MODEL)}")


def print_selected_fields(index, row, selected_fields):
    print("-" * 60)

    # 기본 출력
    print(f"[{index}] 거래처명: {safe_get(row, '거래처명')}")
    print(f"[{index}] 계약일: {safe_get(row, '계약일')}")
    print(f"[{index}] 남은개월: {safe_get(row, '남은개월')}")
    print(f"[{index}] 금액: {safe_get(row, '기본금액')}")
    print(f"[{index}] 주소: {safe_get(row, '주소') or safe_get(row, '주소(실납품주소,도로명주소)')}")
    print(f"[{index}] 자산기번: {safe_get(row, '자산번호')}")
    print(f"[{index}] 시리얼: {safe_get(row, '시리얼번호(기번)')}")
    print(f"[{index}] 키맨: {safe_get(row, '키맨')}")

    # 🔥 추가: 질문 기반 항목 출력
    for field in selected_fields:
        if field not in [
            '거래처명','계약일','남은개월','기본금액',
            '주소','주소(실납품주소,도로명주소)',
            '자산번호','시리얼번호(기번)','키맨'
        ]:
            value = safe_get(row, field)
            if value:
                print(f"[{index}] {clean_label(field)}: {value}")

def print_full_row(index, row):
    print("-" * 60)
    print(f"[{index}] 전체 정보")
    for h in HEADERS:
        print(f"[{index}] {clean_label(h)}: {safe_get(row, h)}")


def print_compact_candidate(index, row):
    print("-" * 60)
    print(f"[{index}] 거래처명: {safe_get(row, COL_COMPANY)}")
    print(f"[{index}] 현장명: {safe_get(row, COL_SITE)}")
    print(f"[{index}] 품목: {safe_get(row, COL_ITEM)}")
    print(f"[{index}] 모델명: {safe_get(row, COL_MODEL)}")


# =========================================================
# 12) 수정 기능
# =========================================================
def is_update_command(question):
    q = normalize_text(question)
    update_words = ["바꿔", "바꿔줘", "수정", "수정해줘", "변경", "변경해줘", "고쳐", "고쳐줘"]
    return any(word in q for word in update_words)


def detect_update_field(question):
    q = normalize_text(question)
    for alias, real_field in UPDATABLE_ALIASES.items():
        if normalize_text(alias) in q:
            return alias, real_field
    return None, None


def extract_new_value_rule(question, detected_label):
    if not detected_label:
        return ""

    original = question.strip()

    patterns = [
        rf"{re.escape(detected_label)}\s*(을|를)?\s*(.+?)\s*(로|으로)\s*(바꿔줘|바꿔|수정해줘|수정|변경해줘|변경|고쳐줘|고쳐)",
        rf"{re.escape(detected_label)}\s*(.+?)\s*(로|으로)\s*(바꿔줘|바꿔|수정해줘|수정|변경해줘|변경|고쳐줘|고쳐)",
    ]

    for pattern in patterns:
        match = re.search(pattern, original)
        if match:
            groups = match.groups()
            for value in groups:
                if value and value not in ["을", "를", "로", "으로", "바꿔줘", "바꿔", "수정해줘", "수정", "변경해줘", "변경", "고쳐줘", "고쳐"]:
                    cleaned = str(value).strip()
                    if cleaned:
                        return cleaned

    temp = original
    temp = temp.replace(detected_label, "", 1)
    temp = re.sub(r"(을|를)", " ", temp)
    temp = re.sub(r"(바꿔줘|바꿔|수정해줘|수정|변경해줘|변경|고쳐줘|고쳐)", " ", temp)
    temp = re.sub(r"(로|으로)", " ", temp)
    temp = " ".join(temp.split())
    return temp.strip()


def get_header_map():
    headers = sheet.row_values(1)
    return {header: idx for idx, header in enumerate(headers, start=1)}


def find_sheet_row_number(target_row):
    all_records = sheet.get_all_records()
    keys_to_check = [COL_COMPANY, COL_SITE, COL_MODEL, COL_ITEM, COL_SERIAL, COL_ASSET]

    for i, record in enumerate(all_records, start=2):
        matched = True
        for key in keys_to_check:
            target_val = normalize_text(target_row.get(key, ""))
            record_val = normalize_text(record.get(key, ""))
            if target_val != record_val:
                matched = False
                break
        if matched:
            return i

    return None


def print_update_report(row, field, old_value, new_value):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n수정 완료")
    print("\n[수정 내역]")
    print(f"거래처명: {safe_get(row, COL_COMPANY)}")
    print(f"현장명: {safe_get(row, COL_SITE)}")
    print(f"수정 항목: {clean_label(field)}")
    print(f"기존 값: {old_value}")
    print(f"변경 값: {new_value}")
    print(f"수정 시간: {now_str}")


# =========================================================
# 13) 질문 기억
# =========================================================
last_search_terms = []
last_entity_text = ""

# =========================================================
# 14) 안내
# =========================================================


# =========================================================
# 15) 메인 루프
# =========================================================
while True:
    question = input("\n질문 입력(종료하려면 exit): ").strip()

    if question.lower() in ["exit", "quit", "종료"]:
        print("PUMI 종료")
        break

    gpt_result = ask_gpt_parser(question, last_entity_text=last_entity_text)

    # ------------------------------
    # A. 수정
    # ------------------------------
    is_update = False
    if gpt_result and gpt_result.get("intent") == "update":
        is_update = True
    elif is_update_command(question):
        is_update = True

    if is_update:
        real_field = None
        field_label = None
        new_value = ""

        if gpt_result:
            gpt_update_field = str(gpt_result.get("update_field", "")).strip()
            mapped = map_field_name(gpt_update_field)
            if mapped in UPDATABLE_ALIASES.values():
                real_field = mapped
                field_label = gpt_update_field
                new_value = str(gpt_result.get("new_value", "")).strip()

        if not real_field:
            field_label, real_field = detect_update_field(question)
            new_value = extract_new_value_rule(question, field_label)

        if not real_field:
            print("수정 가능한 항목은 담당자와 연락처만 가능합니다.")
            continue

        if not new_value:
            print("변경할 값을 이해하지 못했어요.")
            continue

        gpt_entity = str(gpt_result.get("entity", "")).strip() if gpt_result else ""
        search_terms = []
        matches = []

        if gpt_entity:
            matches = find_rows_by_entity(gpt_entity)
            search_terms = [gpt_entity]
        else:
            search_terms = extract_search_terms(question)

            if not search_terms and last_search_terms and is_followup_question(question):
                search_terms = last_search_terms[:]

            matches = find_rows_by_terms(search_terms)

        if not matches and last_search_terms:
            matches = find_rows_by_terms(last_search_terms)
            search_terms = last_search_terms[:]

        if not matches:
            print("수정할 데이터를 찾지 못했어요.")
            continue

        matches.sort(key=lambda row: score_row(row, [real_field], search_terms), reverse=True)
        matches = remove_duplicates(matches)
        target_row = matches[0]
        old_value = target_row.get(real_field, "")

        print("\n[수정 확인]")
        print(f"거래처명: {safe_get(target_row, COL_COMPANY)}")
        print(f"현장명: {safe_get(target_row, COL_SITE)}")
        print(f"수정 항목: {clean_label(real_field)}")
        print(f"기존 값: {old_value}")
        print(f"변경 값: {new_value}")

        confirm = input("수정하시겠습니까? (y/n): ").strip().lower()
        if confirm != "y":
            print("수정 취소")
            continue

        header_map = get_header_map()
        row_number = find_sheet_row_number(target_row)
        col_number = header_map.get(real_field)

        if row_number is None or col_number is None:
            print("시트에서 수정 위치를 찾지 못했어요.")
            continue

        sheet.update_cell(row_number, col_number, new_value)

        data = load_data()
        HEADERS = get_headers()
        HEADER_ALIAS_MAP = build_header_alias_map(HEADERS)

        refreshed = find_rows_by_terms(search_terms) if search_terms else []
        if not refreshed and gpt_entity:
            refreshed = find_rows_by_entity(gpt_entity)

        if refreshed:
            refreshed.sort(key=lambda row: score_row(row, [real_field], search_terms), reverse=True)
            refreshed = remove_duplicates(refreshed)
            target_row = refreshed[0]

        print_update_report(target_row, real_field, old_value, new_value)

        last_search_terms = search_terms[:]
        last_entity_text = find_best_entity_from_match(target_row)
        continue

    # ------------------------------
    # B. 조회
    # ------------------------------
    selected_fields = []
    full_info = is_full_info_request(question)
    current_terms = []
    matches = []
    used_memory = False

    if gpt_result and gpt_result.get("intent") == "query":
        gpt_entity = str(gpt_result.get("entity", "")).strip()
        gpt_fields = map_field_list(gpt_result.get("fields", []))
        full_info = full_info or bool(gpt_result.get("full_info", False))

        header_detected_fields = detect_header_fields_from_question(question)
        rule_fields = map_field_list(get_target_fields_by_rule(detect_question_type(question)))

        selected_fields = unique_keep_order(gpt_fields + header_detected_fields + rule_fields)

        if gpt_entity:
            matches = find_rows_by_entity(gpt_entity)
            current_terms = [gpt_entity]

        if not matches and last_search_terms and is_followup_question(question):
            matches = find_rows_by_terms(last_search_terms)
            current_terms = last_search_terms[:]
            used_memory = True

    if not matches:
        current_terms = extract_search_terms(question)

        if not current_terms and last_search_terms and is_followup_question(question):
            current_terms = last_search_terms[:]
            used_memory = True

        matches = find_rows_by_terms(current_terms)

        rule_fields = map_field_list(get_target_fields_by_rule(detect_question_type(question)))
        header_detected_fields = detect_header_fields_from_question(question)
        selected_fields = unique_keep_order(selected_fields + header_detected_fields + rule_fields)

    if not matches:
        print("관련 데이터를 찾지 못했어요.")
        continue

    matches.sort(key=lambda row: score_row(row, selected_fields, current_terms), reverse=True)
    matches = remove_duplicates(matches)

    last_search_terms = current_terms[:]
    last_entity_text = find_best_entity_from_match(matches[0])

    if used_memory and last_entity_text:
        print(f"\n이전 검색 대상 기억해서 조회함: {last_entity_text}")

    best = matches[0]

    print(f"\n총 {len(matches)}건 찾음")

    if USE_AI_SUMMARY:
        summary = ask_ai_summary(question, matches, selected_fields, full_info)
        if summary:
            print("\nAI 요약")
            print(summary)

    print("\n가장 유력한 결과")
    if full_info:
        print_full_row(1, best)
    elif selected_fields:
        print_selected_fields(1, best, selected_fields)
    else:
        print_selected_fields(1, best, [COL_MANAGER, COL_PHONE])

    if len(matches) > 1:
        print("\n다른 후보")
        for i, row in enumerate(matches[1:4], start=2):
            print_compact_candidate(i, row)
