from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor





table = "yt_api"



def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()



def create_schema(schema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

# שורה זמנית שתמחק את הטבלה הישנה עם העמודה השגויה
    #cur.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")

    if schema =='staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" BIGINT,
                "Likes_Count" BIGINT,
                "Comments_Count" BIGINT
             );
        """
    else:
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" TIME NOT NULL,
                        "Video_Type" VARCHAR(10) NOT NULL, 
                        "Video_Views" BIGINT,
                        "Likes_Count" BIGINT,
                        "Comments_Count" BIGINT
                  );
                """


    cur.execute(table_sql)

    # CREATE TABLE IF NOT EXISTS never changes a table that already exists,
    # so tables created while these columns were INT are upgraded here.
    upgrade_count_columns(cur, schema)

    conn.commit()
    
    close_conn_cursor(conn, cur)


COUNT_COLUMNS = ("Video_Views", "Likes_Count", "Comments_Count")


def upgrade_count_columns(cur, schema):
    """Convert any count column still stored as INT to BIGINT.

    INT tops out at 2,147,483,647, which the channel's biggest videos are approaching.
    Safe to run on every load: once the columns are BIGINT it finds nothing to change.
    """
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = ANY(%s)
          AND data_type = 'integer';
        """,
        (schema, table, list(COUNT_COLUMNS)),
    )
    int_columns = [row["column_name"] for row in cur.fetchall()]

    if int_columns:
        alter_clauses = ", ".join(f'ALTER COLUMN "{col}" TYPE BIGINT' for col in int_columns)
        cur.execute(f"ALTER TABLE {schema}.{table} {alter_clauses};")


def get_video_ids(cur, schema):

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row['Video_ID'] for row in ids]

    return video_ids







    




