import re
import io
import time
import base64
import subprocess
from typing import List
import os

import streamlit as st
from PIL import Image
from openai import OpenAI

# ================================
# 설정
# ================================
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

st.set_page_config(page_title="PUMI AI", layout="centered")
st.title("PUMI AI")

# ================================
# 유틸
# ================================
def optimize_image(uploaded_file, max_width: int = 900, jpeg_quality: int = 55) -> str:

    width, height = image.size
    if width > max_width:
        new_height = int(height * (max_width / width))
        image = image.resize((max_width, new_height))

    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def build_image_contents(files) -> List[dict]:
    contents = []
    for f in files:
        b64 = optimize_image(f)
        contents.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{b64}",
                "detail": "low"
            }
        })
    return contents


def call_openai_with_retry(messages, model="gpt-4o-mini", max_tokens=1600, retries=3):
    last_error = None

    for attempt in range(retries):
        try:
            return client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0,
            )
        except Exception as e:
            last_error = e
            error_text = str(e).lower()

            if "rate limit" in error_text or "429" in error_text:
                wait_sec = 2 + attempt * 2
                time.sleep(wait_sec)
            else:
                raise

    raise last_error


def run_text_search(user_question: str):
    process = subprocess.Popen(
        ["python", "main.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=r"C:\PUMI"
    )
    return process.communicate(input=user_question + "\nexit\n")

import re

def extract_counter_text_from_name_or_placeholder(lines: list[str], keyword: str) -> str:
    for line in lines:
        if keyword in line:
            return line
    return ""


def extract_number_from_line(line: str) -> str:
    if not line:
        return ""
    m = re.search(r'(\d[\d,]*)', line)
    return m.group(1) if m else ""


def normalize_counter_text(raw_text: str) -> list[str]:
    text = raw_text.replace("\r", "\n")
    lines = [x.strip() for x in text.split("\n") if x.strip()]
    return lines


def parse_hanjo_counter_text(raw_text: str) -> dict:
    lines = normalize_counter_text(raw_text)

    total_line = extract_counter_text_from_name_or_placeholder(lines, "장치카운트")
    black_a4_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4")
    color_a4_line = extract_counter_text_from_name_or_placeholder(lines, "칼라 A4")
    color_a3_line = extract_counter_text_from_name_or_placeholder(lines, "큰컬 A3")

    black_a4_a3_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4+A3")
    black_a4_a3x2_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4+A3x2")

    total = extract_number_from_line(total_line)
    black = extract_number_from_line(black_a4_line)
    color = extract_number_from_line(color_a4_line)
    color_large = extract_number_from_line(bigcolor_a3_line)

    if not black:
        black = extract_number_from_line(black_a4_a3_line) or extract_number_from_line(black_a4_a3x2_line)

    return {
        "흑": black,
        "컬": color,
        "큰컬": color_large,
        "합": total,
    }


def format_counter_line(counter_map: dict) -> str:
    return f"매수: 흑-{counter_map.get('흑','')} 컬-{counter_map.get('컬','')} 큰컬-{counter_map.get('큰컬','')} 합-{counter_map.get('합','')}"

def extract_counter_text_from_name_or_placeholder(lines: list[str], keyword: str) -> str:
    """
    lines 중 keyword가 들어간 줄을 찾아 반환
    """
    for line in lines:
        if keyword in line:
            return line
    return ""


def extract_number_from_line(line: str) -> str:
    """
    '블랙 A4 392' 같은 줄에서 숫자만 추출
    쉼표 포함 숫자 지원
    """
    if not line:
        return ""
    m = re.search(r'(\d[\d,]*)', line)
    return m.group(1) if m else ""


def normalize_counter_text(raw_text: str) -> list[str]:
    """
    OCR/AI에서 읽은 텍스트를 줄 단위로 정리
    """
    text = raw_text.replace("\r", "\n")
    lines = [x.strip() for x in text.split("\n") if x.strip()]
    return lines


def parse_hanjo_counter_text(raw_text: str) -> dict:
    """
    한조 카운터 텍스트에서 숫자만 뽑아 규칙대로 매핑
    규칙:
    - 합 = 장치카운트
    - 흑 = 블랙 A4
    - 컬 = 칼라 A4
    - 큰컬 = 칼라 A3
    """
    lines = normalize_counter_text(raw_text)

    total_line = extract_counter_text_from_name_or_placeholder(lines, "장치카운트")
    black_a4_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4")
    color_a4_line = extract_counter_text_from_name_or_placeholder(lines, "칼라 A4")
    color_a3_line = extract_counter_text_from_name_or_placeholder(lines, "칼라 A3")

    # 보조값
    black_a4_a3_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4+A3")
    black_a4_a3x2_line = extract_counter_text_from_name_or_placeholder(lines, "블랙 A4+A3x2")

    total = extract_number_from_line(total_line)
    black = extract_number_from_line(black_a4_line)
    color = extract_number_from_line(color_a4_line)
    color_large = extract_number_from_line(color_a3_line)

    # 흑 값이 없을 때만 보조값 사용
    if not black:
        black = extract_number_from_line(black_a4_a3_line) or extract_number_from_line(black_a4_a3x2_line)

    return {
        "흑": black,
        "컬": color,
        "큰컬": color_large,
        "합": total,
    }


def format_counter_line(counter_map: dict) -> str:
    return f"매수: 흑-{counter_map.get('흑','')} 컬-{counter_map.get('컬','')} 큰컬-{counter_map.get('큰컬','')} 합-{counter_map.get('합','')}"
# ================================
# 프롬프트
# ================================
BASIC_PROMPT = """
이미지를 분석해서 아래 양식으로 작성해줘.
보이지 않거나 확인이 어려운 항목은 공란으로 둔다.
설명 없이 양식만 출력한다.

[중요]
- 기기가 여러 대면 1., 2., 3. 형식으로 각각 구분해서 작성한다.
- 공통 정보(업체명, 부서명, 지역, 키맨/접수자)는 상단에 1번만 작성한다.
- 기기별 정보는 각 번호 아래에만 작성한다.

거래처명:
계약일:
남은개월:
금액:
주소:
키맨:

-------------------------------------
1.
모델명:
시리얼:
자산기번:

2.
모델명:
시리얼:
자산기번:
"""


INSPECTION_PROMPT = """
이미지를 보고 아래 점검 양식을 작성해줘.

[공통 규칙]
- 보이지 않거나 확인이 어려운 항목은 공란으로 둔다
- 설명문, 따옴표, ... 같은 불필요한 문자는 절대 넣지 않는다
- 값이 없는 항목에 "공란"이라는 텍스트를 쓰지 않는다
- 값이 없으면 항목은 유지하고 값만 비워둔다
- 기기가 여러 대면 1., 2., 3. 형식으로 각각 따로 작성한다
- 공통 정보는 상단 1번만 작성하고, 기기별 정보는 번호 섹션 안에 쓴다
- 부품신청 아래부터는 원래 양식을 그대로 유지한다
- 캘린더/일정 화면이 포함되어 있으면 날짜와 시간 정보를 읽어서 도착 시간과 소요 시간에 반영한다
- 시작 시간과 종료 시간이 모두 보이면 소요 시간을 계산한다
- 일정 제목, 방문 시간, 예약 시간이 보이면 시간 정보로 우선 활용한다
- 종료 시간이 없으면 소요 시간은 공란으로 둔다

[공통 정보 규칙]
- 구분: 점검 또는 AS. 이미지상 점검이면 점검, AS면 AS
- 레벨: 공란
- 등급:
  - 업체명 앞에 숫자+영문 코드가 있으면 숫자는 버리고 영문만 쓴다
  - 예: 3N업체명 -> 등급 N
  - 예: 4SS선도에프엠주식회사 -> 등급 SS
- 업체명: 등급 코드를 제거한 순수 업체명만 작성
- 부서명: 층수와 그 뒤 위치를 작성 (예: 지하1층 방재실)
- 지역: 시/군까지만 작성 (예: 경기 평택시)
- 키맨/접수자: 이름 + 연락처 포함, 중복 제거, 여러 명이면 모두 표기
- 주차비지원유무: 공란

[기기별 규칙]
- 모델명: 모델만 작성 (예: SL-X4220RX, D450)
- 시리얼넘버: 시리얼만 작성
- 자산기번: 공란
- 내용: 공란
- 처리내용: 공란

[매수 절대 규칙]
- 장치카운트 = 합
- 블랙 A4 = 흑
- 칼라 A4 = 컬
- 칼라 A3 = 큰컬
- "블랙 A4+A3", "블랙 A4+A3x2" 같은 값은 흑 값이 아예 없을 때만 보조값으로 사용한다
- "A4"라는 단어만 보고 판단하지 말고 반드시 "블랙"인지 "칼라"인지 함께 확인해서 넣는다
- 블랙 A4를 컬에 넣으면 안 된다
- 칼라 A4를 흑에 넣으면 안 된다
- 장치카운트는 반드시 합으로만 넣는다
- 값이 없으면 비워둔다
- "공란"이라는 단어는 쓰지 않는다
- 형식은 반드시 아래처럼:
  매수: 흑-111324 컬-27036 큰컬-1200 합-138360
- 예:
  - 칼라 A4만 있고 칼라 A3가 없으면 → 매수: 흑-111324 컬-27036 큰컬- 합-138360
  - 칼라 A4와 칼라 A3 둘 다 없으면 → 매수: 흑-111324 컬- 큰컬- 합-138360
- 한조 사진이 없으면:
  매수: 흑- 컬- 큰컬- 합-

[토너잔량 규칙]
- 토너잔량은 K/C/M/Y 순서로만 작성한다
- 형식:
  토너잔량: K-51 C-52 M-48 Y-27
- 값이 없으면 비워둔다
- 형식:
  토너잔량: K- C- M- Y-

[기타 규칙]
- 폐통: 공란
- 여분: 미확인
- 한틴이카유무:
  - 한조/카운터 사진이 보이면 유
  - 안 보이면 무
- 특이사항: 없으면 공란

[중요 규칙 - 한조/카운터 해석]

1. "장치카운트"는 총합(합계)이다.
2. "블랙 A4"는 흑(흑백)이다.
3. "칼라 A4"는 컬이다.
4. "칼라 A3"는 큰컬이다.
5. 블랙 A3 / 블랙 A4+A3 등은 무시한다.

6. 매수 계산 규칙:
- 흑 = 블랙 A4
- 컬 = 칼라 A4
- 큰컬 = 칼라 A3
- 합 = 장치카운트

7. 값이 없으면 공란으로 둔다 (0 금지)

8. 절대 서로 값을 복사하지 말 것
(예: 흑 값을 컬에 넣지 말 것)

9. 출력은 반드시:
매수: 흑-숫자 컬-숫자 큰컬-숫자 합-숫자

[출력 양식]
작성자:
구분:
레벨:
등급:
업체명:
부서명:
지역:
키맨/접수자:
-------------------------------------
1.
모델명:
시리얼넘버:
자산기번:
내용:
처리내용:
매수: 흑- 컬- 큰컬- 합-
토너잔량: K- C- M- Y-
폐통:
여분:
한틴이카유무:
주차비지원유무:
특이사항:
-------------------------------------
2.
모델명:
시리얼넘버:
자산기번:
내용:
처리내용:
매수: 흑- 컬- 큰컬- 합-
토너잔량: K- C- M- Y-
폐통:
여분:
한틴이카유무:
주차비지원유무:
특이사항:
-------------------------------------
※부품신청※
보증기간 내 여부:
교체 전 카운터 누적 사용매수:
사용 부품 예상 사용매수:
▶ 신청 부품
물품명:
수량:
출고여부:
-------------------------------------
※자가신청※
물품:
수량:
출고여부:
-------------------------------------
도착 시간:
소요 시간:
"""

# ================================
# UI
# ================================
question = st.text_input("질문 입력")

# 📌 업로드
uploaded_files = st.file_uploader(
    "사진 업로드",
    type=["jpg", "jpeg", "png"],
    accept_multiple_files=True
)

# 🔥 여기로 빼야 함 (if 밖으로!)
st.subheader("양식 선택")
mode = st.radio("", ["기존 양식", "점검 양식"], horizontal=True)

# 📌 버튼
run_clicked = st.button("🚀 검색", key="run_btn", use_container_width=True)

# 📌 업로드된 사진 표시 (이건 아래로)
if uploaded_files:
    with st.expander(f"📸 업로드된 사진 ({len(uploaded_files)}장)"):
        for f in uploaded_files:
            st.image(f, caption=f.name, use_container_width=True)

# ================================
# 실행
# ================================
if run_clicked:
    if uploaded_files:
        # 한조 키워드 감지
        joined_text = question.lower() if question else ""

        if any(x in joined_text for x in ["한조", "카운터", "장치카운트"]):
            prompt_text = INSPECTION_PROMPT + "\n\n[강제모드] 반드시 한조 규칙을 최우선 적용할 것."
        else:
            prompt_text = BASIC_PROMPT if mode == "기존 양식" else INSPECTION_PROMPT

        with st.spinner("사진 분석 중..."):
            try:
                image_contents = build_image_contents(uploaded_files)

                messages = [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": prompt_text}] + image_contents
                    }
                ]

                response = call_openai_with_retry(
                    messages=messages,
                    model="gpt-4o-mini",
                    max_tokens=900,
                    retries=2
                )

                result = response.choices[0].message.content or ""

                st.subheader("📋 생성 결과")
                st.text_area(
                    "결과",
                    value=result if result else "없음",
                    height=500,
                    key="result_box"
                )

            except Exception as e:
                st.subheader("에러")
                st.text(str(e))

    elif question:
        with st.spinner("검색 중..."):
            try:
                output, error = run_text_search(question)

                st.subheader("🔎 검색 결과")
                st.text_area(
                    "검색 결과",
                    value=output if output else "없음",
                    height=500,
                    key="search_result_box"
                )

                if error:
                    st.subheader("에러")
                    st.text(error)

            except Exception as e:
                st.subheader("에러")
                st.text(str(e))

    elif question:
        with st.spinner("검색 중..."):
            try:
                output, error = run_text_search(question)

                st.subheader("🔎 검색 결과")
                st.text_area(
    "검색 결과",
    value=output if output else "없음",
    height=500
)

                if error:
                    st.subheader("에러")
                    st.text(error)

            except Exception as e:
                st.subheader("에러")
                st.text(str(e))
    else:
        st.warning("질문을 입력하거나 사진을 업로드해줘.")
