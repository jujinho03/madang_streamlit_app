import streamlit as st
import pandas as pd
import time
import duckdb
import os
import atexit

# --- 1. 앱이 실행될 때 DB 파일이 있는지 확인하고, 없으면 생성 ---
DB_FILE = 'madang.db'

# @st.cache_resource는 DB 연결을 캐시(저장)하여 앱 속도를 높입니다.
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
        # st.info(f"'{DB_FILE}' 파일을 찾을 수 없습니다. CSV 파일로부터 새로 생성합니다...") # <-- 이 줄을 주석 처리했습니다.
        
        # CSV 파일로부터 DB 테이블 생성
        try:
            conn.sql("CREATE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            conn.sql("CREATE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
            conn.sql("CREATE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
            # st.success(f"'{DB_FILE}' 생성 완료!") # <-- 이 줄을 주석 처리했습니다.
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
    쓰기 작업은 캐시된 연결 대신 새 연결을 열어 즉시 반영합니다.
    """
    conn = duckdb.connect(database=DB_FILE, read_only=False)
    conn.execute(sql_query)
    conn.close()
    
    # 모든 캐시를 지워서, 다음번 조회 시
    # 방금 입력한 내용을 다시 읽어오도록 합니다.
    st.cache_data.clear()
    st.cache_resource.clear() # DB 연결 캐시도 초기화

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

# --- Tab 1: 고객 조회 (개선됨) ---
with tab1:
    st.subheader("고객 주문내역 조회")
    name_input = st.text_input("고객명 입력:", key="tab1_name_input")
    
    if len(name_input) > 0:
        # 1. 먼저 Customer 테이블에서 고객이 존재하는지 확인
        cust_data_tab1 = query_db(f"SELECT custid FROM Customer WHERE name = '{name_input}' LIMIT 1")
        
        if cust_data_tab1:
            # 2. 고객이 존재하면, 주문 내역(Orders)을 JOIN으로 조회
            sql = f"select c.custid, c.name, b.bookname, o.orderdate, o.saleprice from Customer c, Book b, Orders o \
                    where c.custid = o.custid and o.bookid = b.bookid and name = '{name_input}';"
            
            result_data = query_db(sql)
            
            if result_data:
                # 주문 내역이 있으면 DataFrame으로 표시
                result_df = pd.DataFrame(result_data)
                st.dataframe(result_df)
            else:
                # 주문 내역이 없으면 (신규 등록 고객) 메시지 표시
                st.info(f"'{name_input}' 님은 등록된 고객이지만, 아직 주문 내역이 없습니다.")
        else:
            # Customer 테이블에도 없으면
            st.warning(f"'{name_input}' 고객은 등록되지 않았습니다. '거래 입력' 탭에서 신규 등록할 수 있습니다.")

# --- Tab 2: 거래 입력 (과제 기능 추가됨) ---
with tab2:
    st.subheader("신규 거래 입력")
    
    # 고객 이름으로 custid 찾기
    name_for_order = st.text_input("거래할 고객명:", key="tab2_name_input")
    custid = None
    
    if len(name_for_order) > 0:
        cust_data_tab2 = query_db(f"SELECT custid FROM Customer WHERE name = '{name_for_order}' LIMIT 1")
        
        if cust_data_tab2:
            # --- [A] 기존 고객인 경우 ---
            custid = cust_data_tab2[0]['custid']
            st.success(f"'{name_for_order}' 님의 고객번호({custid})가 확인되었습니다.")
            
            # 기존 거래 입력 로직
            select_book = st.selectbox("구매 서적:", books, key="selectbox_books")
            price = st.text_input("금액:", key="price_input")
            
            if st.button('거래 입력', key="submit_button"):
                if select_book is not None and price and price.isdigit():
                    try:
                        bookid = select_book.split(",")[0]
                        dt = time.strftime('%Y-%m-%d', time.localtime())
                        
                        orderid_result = query_db("select max(orderid) as max_id from orders;")
                        orderid = (orderid_result[0]['max_id'] or 0) + 1 # max_id가 None일 경우 0으로 처리
                        
                        sql_insert = f"insert into orders (orderid, custid, bookid, saleprice, orderdate) values ({orderid}, {custid}, {bookid}, {price}, '{dt}');"
                        run_query(sql_insert)
                        
                        st.success('거래가 입력되었습니다!')
                        
                    except Exception as e:
                        st.error(f"거래 입력 중 오류 발생: {e}")
                else:
                    st.error("구매 서적을 선택하고, 금액을 숫자로 입력해주세요.")

        else:
            # --- [B] 신규 고객인 경우 (과제 핵심) ---
            st.info(f"'{name_for_order}' 님은 신규 고객입니다. 아래 정보를 입력 후 등록해주세요.")
            
            new_address = st.text_input("주소:", key="new_addr")
            new_phone = st.text_input("전화번호:", key="new_phone", placeholder="000-0000-0000")
            
            if st.button("신규 고객 등록하기", key="register_btn"):
                if not new_address:
                    st.error("주소를 반드시 입력해야 합니다.")
                else:
                    try:
                        # 1. 새 고객번호(custid) 생성 (max + 1)
                        new_custid_result = query_db("SELECT max(custid) as max_id FROM Customer")
                        new_custid = (new_custid_result[0]['max_id'] or 0) + 1 # max_id가 None일 경우 0으로 처리
                        
                        # 2. Customer 테이블에 INSERT
                        sql_insert_cust = f"INSERT INTO Customer (custid, name, address, phone) VALUES ({new_custid}, '{name_for_order}', '{new_address}', '{new_phone}')"
                        run_query(sql_insert_cust)
                        
                        st.success(f"고객 {name_for_order}님 (고객번호: {new_custid}) 등록 완료!")
                        st.info("페이지가 새로고침됩니다. 다시 고객명을 입력하여 거래를 진행하세요.")
                        
                        # 앱을 새로고침하여 등록된 고객 정보를 다시 불러오게 함
                        time.sleep(2) # 2초 대기
                        st.rerun() 
                        
                    except Exception as e:
                        st.error(f"신규 고객 등록 중 오류 발생: {e}")