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
        # st.info(...) 메시지는 교수님 요청으로 제거되었습니다.
        
        # CSV 파일로부터 DB 테이블 생성
        try:
            conn.sql("CREATE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            conn.sql("CREATE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
            conn.sql("CREATE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
            # st.success(...) 메시지는 교수님 요청으로 제거되었습니다.
        except Exception as e:
            st.error(f"DB 테이블 생성 실패: {e}")
            conn.close()
            st.stop()

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
    """
    conn = duckdb.connect(database=DB_FILE, read_only=False)
    conn.execute(sql_query)
    conn.close()
    
    # 캐시를 지워서 다음번 조회 시 최신 데이터를 반영합니다.
    st.cache_data.clear()
    st.cache_resource.clear()

# --- 3. Streamlit 앱 본체 ---

st.title("마당서점 신규 고객 등록 🧑‍💻")

# 앱 시작 시 DB 연결 초기화
try:
    get_db_conn()
except Exception as e:
    st.error(f"데이터베이스 연결에 실패했습니다: {e}")
    st.stop()

# 도서 목록 불러오기
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
    name_input_tab1 = st.text_input("고객명 입력:", key="tab1_name_input")
    
    if len(name_input_tab1) > 0:
        cust_data_tab1 = query_db(f"SELECT custid FROM Customer WHERE name = '{name_input_tab1}' LIMIT 1")
        
        if cust_data_tab1:
            sql = f"select c.custid, c.name, b.bookname, o.orderdate, o.saleprice from Customer c, Book b, Orders o \
                    where c.custid = o.custid and o.bookid = b.bookid and name = '{name_input_tab1}';"
            result_data = query_db(sql)
            
            if result_data:
                result_df = pd.DataFrame(result_data)
                st.dataframe(result_df)
            else:
                st.info(f"'{name_input_tab1}' 님은 등록된 고객이지만, 아직 주문 내역이 없습니다.")
        else:
            st.warning(f"'{name_input_tab1}' 고객은 등록되지 않았습니다. '거래 입력' 탭에서 신규 등록할 수 있습니다.")

# --- Tab 2: 거래 입력 (교수님 과제 로직) ---
with tab2:
    st.subheader("신규 거래 입력")
    
    name_input_tab2 = st.text_input("거래할 고객명:", key="tab2_name_input")
    
    custid = None
    is_new_customer = False # 신규 고객인지 확인하는 플래그

    if len(name_input_tab2) > 0:
        cust_data_tab2 = query_db(f"SELECT custid FROM Customer WHERE name = '{name_input_tab2}' LIMIT 1")
        
        if cust_data_tab2:
            # --- [A] 기존 고객인 경우 ---
            custid = cust_data_tab2[0]['custid']
            
        else:
            # --- [B] 신규 고객인 경우 (과제 핵심) ---
            try:
                # 1. 새 고객번호(custid) 생성 (max + 1)
                new_custid_result = query_db("SELECT max(custid) as max_id FROM Customer")
                new_custid = (new_custid_result[0]['max_id'] or 0) + 1 # max_id가 None일 경우 0으로 처리
                
                custid = new_custid
                is_new_customer = True # 신규 고객 플래그 설정
                
                
            except Exception as e:
                st.error(f"신규 고객번호 생성 실패: {e}")
                custid = None # 오류 시 custid를 None으로 되돌림

    # 고객이 확인된 경우 (기존이든, 신규든)
    if custid is not None:
        select_book = st.selectbox("구매 서적:", books, key="selectbox_books")
        price = st.text_input("금액:", key="price_input")
        
        if st.button('거래 입력', key="submit_button"):
            if select_book is not None and price and price.isdigit():
                try:
                    # 1. (신규 고객이라면) Customer 테이블에 먼저 INSERT
                    if is_new_customer:
                        # 주소(address)와 전화번호(phone)는 'NULL'로 임의 설정
                        sql_insert_cust = f"INSERT INTO Customer (custid, name, address, phone) VALUES ({custid}, '{name_input_tab2}', NULL, NULL)"
                        run_query(sql_insert_cust)

                    # 2. Orders 테이블에 거래 내역 INSERT
                    bookid = select_book.split(",")[0]
                    dt = time.strftime('%Y-%m-%d', time.localtime())
                    
                    orderid_result = query_db("select max(orderid) as max_id from orders;")
                    orderid = (orderid_result[0]['max_id'] or 0) + 1 
                    
                    sql_insert_order = f"insert into orders (orderid, custid, bookid, saleprice, orderdate) values ({orderid}, {custid}, {bookid}, {price}, '{dt}');"
                    run_query(sql_insert_order)
                    
                    st.success('거래가 입력되었습니다!')
                    
                except Exception as e:
                    st.error(f"거래 입력 중 오류 발생: {e}")
            else:
                st.error("구매 서적을 선택하고, 금액을 숫자로 입력해주세요.")