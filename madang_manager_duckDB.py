import streamlit as st
import duckdb
import pandas as pd
import time

# -----------------------------
# 1. DuckDB(madang.db) 연결
# -----------------------------
# madangDB_Creation.ipynb 실행 후 같은 폴더에 생성된 madang.db 사용
conn = duckdb.connect("madang.db")


def query_df(sql: str) -> pd.DataFrame:
    """SELECT 결과를 DataFrame으로 반환"""
    return conn.execute(sql).df()


def execute(sql: str):
    """INSERT/UPDATE/DELETE 실행용"""
    conn.execute(sql)


# -----------------------------
# 2. 화면 제목
# -----------------------------
st.title("마당 서점 관리 시스템")

# -----------------------------
# 3. 책 목록 불러와서 드롭다운용 리스트 만들기
# -----------------------------
books_df = query_df("SELECT bookid, bookname FROM Book ORDER BY bookid;")

books = [None]
for _, row in books_df.iterrows():
    books.append(f"{int(row['bookid'])}. {row['bookname']}")


# -----------------------------
# 4. 탭 구성
# -----------------------------
tab1, tab2 = st.tabs(["고객 조회", "거래 입력"])

# ===== 탭1: 고객 조회 =====
with tab1:
    st.subheader("고객별 구매 내역 조회")

    name = st.text_input("고객명", placeholder="예: 홍길동")

    if len(name) > 0:
        sql = f"""
        SELECT c.custid,
               c.name,
               b.bookname,
               strftime(o.orderdate, '%Y-%m-%d') AS orderdate,
               o.saleprice
        FROM Customer c
        JOIN Orders o ON c.custid = o.custid
        JOIN Book b ON o.bookid = b.bookid
        WHERE c.name = '{name}'
        ORDER BY o.orderdate;
        """
        result = query_df(sql)

        if result.empty:
            st.warning(f"'{name}' 고객의 구매 내역이 없습니다.")
        else:
            #st.success(f"'{name}' 고객의 구매 내역입니다.")
            st.dataframe(result, use_container_width=True)

# ===== 탭2: 거래 입력 =====
with tab2:
    st.subheader("새 거래 입력")

    # 1) 고객 이름 입력 (여기에 '윤수연' 등 본인 이름 넣어서 실습)
    customer_name = st.text_input("고객명")

    # 2) 구매 서적 선택
    selected_book = st.selectbox("구매 서적 선택", books)

    # 3) 금액 입력
    price = st.number_input("금액(원)", min_value=0, step=1000)

    # 4) '저장' 버튼
    if st.button("거래 저장"):
        if (not customer_name) or (selected_book is None) or (price <= 0):
            st.error("고객명, 서적, 금액을 모두 입력해야 합니다.")
        else:
            # 고객 ID 새로 부여 (현재 max(custid)+1)
            cust_df = query_df("SELECT COALESCE(MAX(custid), 0) AS maxid FROM Customer;")
            new_custid = int(cust_df["maxid"][0]) + 1

            # 같은 이름이 이미 있으면 그 사람 custid 재사용
            exist_df = query_df(
                f"SELECT custid FROM Customer WHERE name = '{customer_name}';"
            )
            if not exist_df.empty:
                custid = int(exist_df["custid"][0])
            else:
                custid = new_custid
                insert_customer_sql = f"""
                INSERT INTO Customer(custid, name, address, phone)
                VALUES ({custid}, '{customer_name}', 'Seoul', '010-0000-0000');
                """
                execute(insert_customer_sql)

            # bookid 추출 ("1. 축구의 역사" → 1)
            bookid = int(str(selected_book).split(".")[0])

            # orderid = max(orderid)+1
            order_df = query_df("SELECT COALESCE(MAX(orderid), 0) AS maxid FROM Orders;")
            new_orderid = int(order_df["maxid"][0]) + 1

            # 오늘 날짜
            dt = time.localtime()
            orderdate = time.strftime("%Y-%m-%d", dt)

            # Orders 테이블에 INSERT
            insert_order_sql = f"""
            INSERT INTO Orders(orderid, custid, bookid, saleprice, orderdate)
            VALUES ({new_orderid}, {custid}, {bookid}, {int(price)}, '{orderdate}');
            """
            execute(insert_order_sql)

            st.success(f"새 거래가 저장되었습니다! (고객명: {customer_name})")

            st.info("👉 탭1 '고객 조회'에서 방금 입력한 고객명을 검색해보세요.")
