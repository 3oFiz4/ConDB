import time, os
import module.db.DataOperation as DataOperation
import module.db.Initialization as Initialization
import module.db.LocalCache as LocalCache

def main():
    # --- setup ---
    DB_URL = os.getenv("db_url")
    DB_KEY = os.getenv("db_key")
    TABLE_NAME = "Test"           # table name in Supabase
    LOCAL_DIR = "./cache"          # local cache directory

    # create connection
    conn = Initialization(DB_URL, DB_KEY, TABLE_NAME, LOCAL_DIR)
    op = DataOperation(conn)

    # --- test push ---
    sample_entry = {
        "id": "test_" + str(int(time.time())),
        "name": "sample_item",
        "time": time.ctime()
    }

    push_result = op.push(sample_entry)
    print("Push Result:", push_result)

    # --- test pull ---
    pull_result = op.pull(limit=5)
    print("Pull Result:", pull_result)


if __name__ == "__main__":
    main()