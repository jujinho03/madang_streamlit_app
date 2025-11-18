import streamlit as st
import pandas as pd
import time
import duckdb
import os
import atexit

# --- 1. 앱이 실행될 때 DB 파일이 있는지 확인하고, 없으면 생성 ---
DB_FILE = 'madang.db'

# @st.cache_resource는 DB 연결을 캐시(저장)하여 앱 속도를 높입니다.
# Streamlit이 앱을 재실행할 때마다 새로 연결하지 않게 해줍니다.
@st.cache_resource
def get_db_conn():
    """
    DuckDB에 연결하고, madang.db 파일이 없으면 CSV에서 생성합니다.
    """
    db_file_exists = os.path.exists(DB_FILE)
    
    # DB에 연결합니다 (파일이 없으면 새로 생성됩니다)
    conn = duckdb.connect(database=DB_FILE, read_only=False)
    
    # 파일이 처음 생성된 경우 (혹은 테이블이 없는 경우)
    if not db_file_exists:
        
        # CSV 파일로부터 DB 테이블 생성
        try:
            conn.sql("CREATE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            conn.sql("CREATE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
            conn.sql("CREATE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
            st.success(f"'{DB_FILE}' 생성 완료!")
        except Exception as e:
            st.error(f"DB 테이블 생성 실패: {e}")
            # 생성에 실패하면 앱을 중지시킵니다.
            conn.close()
            st.stop()

    # 정상적으로 연결된 객체를 반환합니다.
    return conn

# --- 2. 쿼리 함수 정의 ---

def query_db(sql_query, return_type='dict'):
    """
    SELECT (읽기) 쿼리를 실행하고 결과를 반환합니다.
    캐시된 연결을 사용합니다.
    """
    conn = get_db_conn()
    result_data = conn.execute(sql_query)
    
    result = None
    if return_type == 'df':
        result = result_data.df()
    elif return_type == 'dict':
        result = result_data.fetch_df().to_dict('records')
    else:
        result = result_data.fetchall()
    
    return result

def run_query(sql_query):
    """
    INSERT/UPDATE (쓰기) 쿼리를 실행합니다.
    DuckDB는 연결을 닫을 때 파일에 최종 저장(checkpoint)하므로,
    쓰기 작업은 캐시된 연결 대신 새 연결을 열어 즉시 반영합니다.
    """
    conn = duckdb.connect(database=DB_FILE, read_only=False)
    conn.execute(sql_query)
    conn.close()
    
    # SELECT 쿼리에 사용되는 캐시를 지워서, 다음번 조회 시
    # 방금 입력한 내용을 다시 읽어오도록 합니다.
    st.cache_data.clear()

# --- 3. Streamlit 앱 본체 ---

st.title("마당서점 신규 고객 등록 🧑‍💻")

# 앱 시작 시 DB 연결 초기화 및 생성 확인
try:
    get_db_conn()
except Exception as e:
    st.error(f"데이터베이스 연결에 실패했습니다: {e}")
    st.stop()

# 도서 목록 불러오기 (@st.cache_data: 이 함수의 결과값을 캐시)
@st.cache_data
def load_books():
    books = [None]
    try:
        result_list = query_db("select concat(bookid, ',', bookname) as book_info from Book")
        for res in result_list:
            books.append(res['book_info'])
    except Exception as e:
        st.error(f"Book 테이블 로드 실패: {e}")
    return books

books = load_books()

# 탭 생성
tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# --- Tab 1: 고객 조회 ---
with tab1:
    st.subheader("고객 주문내역 조회")
    name_input = st.text_input("고객명 입력:")
    
    if len(name_input) > 0:
        # F-string과 {name_input}을 사용하여 SQL 쿼리를 안전하게 만듭니다.
        sql = f"select c.custid, c.name, b.bookname, o.orderdate, o.saleprice from Customer c, Book b, Orders o \
                where c.custid = o.custid and o.bookid = b.bookid and name = '{name_input}';"
        
        result_data = query_db(sql)
        
        if result_data:
            result_df = pd.DataFrame(result_data)
            st.dataframe(result_df)
        else:
            st.warning(f"'{name_input}' 고객의 주문 내역이 없습니다.")

# --- Tab 2: 거래 입력 ---
with tab2:
    st.subheader("신규 거래 입력")
    
    # 고객 이름으로 custid 찾기
    name_for_order = st.text_input("거래할 고객명:")
    custid = None
    
    if len(name_for_order) > 0:
        cust_data = query_db(f"SELECT custid FROM Customer WHERE name = '{name_for_order}' LIMIT 1")
        if cust_data:
            custid = cust_data[0]['custid']
            st.success(f"'{name_for_order}' 님의 고객번호({custid})가 확인되었습니다.")
        else:
            st.error(f"'{name_for_order}' 고객을 찾을 수 없습니다.")

    # 고객이 확인된 경우에만 나머지 입력 필드 표시
    if custid is not None:
        select_book = st.selectbox("구매 서적:", books, key="selectbox_books")
        price = st.text_input("금액:", key="price_input")
        
        if st.button('거래 입력', key="submit_button"):
            if select_book is not None and price and price.isdigit():
                try:
                    bookid = select_book.split(",")[0]
                    dt = time.strftime('%Y-%m-%d', time.localtime())
                    
                    # 새 주문번호 생성
                    orderid_result = query_db("select max(orderid) as max_id from orders;")
                    orderid = orderid_result[0]['max_id'] + 1
                    
                    # INSERT 쿼리 실행
                    sql_insert = f"insert into orders (orderid, custid, bookid, saleprice, orderdate) values ({orderid}, {custid}, {bookid}, {price}, '{dt}');"
                    run_query(sql_insert) # 쓰기 함수 실행
                    
                    st.success('거래가 입력되었습니다!')
                    
                except Exception as e:
                    st.error(f"거래 입력 중 오류 발생: {e}")
            else:
                st.error("구매 서적을 선택하고, 금액을 숫자로 입력해주세요.")