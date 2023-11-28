from sqlalchemy.orm import declarative_base
import sqlalchemy
from sqlalchemy import Index
from sqlalchemy.orm import sessionmaker
import json

f = open('slow_query/files/data_source.json', encoding='utf-8')
data_load = json.load(f)
dsn = f"postgresql://{data_load['pg_data']['user']}:{data_load['pg_data']['password_pg']}@{data_load['pg_data']['ip_addr']}:5432/{data_load['pg_data']['database']}"
engine = sqlalchemy.create_engine(dsn)
Base = declarative_base()
Session = sessionmaker(bind=engine)

class SlowQuery(Base):
    __tablename__ = "slow_query_table"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key= True)
    client_addr = sqlalchemy.Column(sqlalchemy.String(length=15), unique= False)
    pid = sqlalchemy.Column(sqlalchemy.Integer, unique= False)
    query = sqlalchemy.Column(sqlalchemy.Text, unique= False)
    state = sqlalchemy.Column(sqlalchemy.String(length=100), unique= False)
    duration = sqlalchemy.Column(sqlalchemy.Float, unique= False)
    quantity = sqlalchemy.Column(sqlalchemy.String(length=100), unique= False)

    __table_args__ = (
        Index('idx_client_addr', "client_addr"), 
        Index('idx_pid', "pid"), 
        # Index('idx_query', "query"), 
        Index('idx_state', "state"), 
        Index('idx_duration', "duration"), 
        Index('idx_quantity', "quantity"), 
    )

    def __str__(self) -> str:
        return f'{self.id}: {self.client_addr},{self.pid},{self.query},{self.state},{self.duration},{self.quantity}'
    
def create_tables(engine: str):
    Base.metadata.create_all(engine)

def select_actual_data_2() -> list:
    resultset: list = []
    with Session() as session:
        my_query = session.query(SlowQuery.client_addr, SlowQuery.pid, SlowQuery.query,SlowQuery.state,
                                 SlowQuery.duration, SlowQuery.quantity)
        for lines in my_query:
            resultset.append(lines)
    return resultset

def insert_data_merge(client_addr: str = None, pid: int = None, query: str = None, state: str = None,
                duration: float = None, quantity: str = None, ) -> None:
    '''
    для записи без дублей
    '''
    user_data = SlowQuery(client_addr=client_addr, pid=pid, query=query, 
                          state=state, duration=duration, quantity=quantity)
    with Session() as session:
        session.merge(user_data)
        session.commit()

def insert_data(client_addr: str = None, pid: int = None, query: str = None, state: str = None,
                duration: float = None, quantity: str = None, ) -> None:
    '''
    для первой выгрузки, когда таблица пуста
    '''
    user_data = SlowQuery(client_addr=client_addr, pid=pid, query=query, 
                          state=state, duration=duration, quantity=quantity)
    with Session() as session:
        session.add(user_data)
        session.commit()


if __name__ == "__main__":
    # create_tables(engine)
    # data = select_actual_data_2(dsn)
    print(__file__)
