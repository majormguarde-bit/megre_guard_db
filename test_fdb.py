import fdb
import sys

print("Testing fdb connection...")
try:
    # Try to connect to a dummy file to see the error
    # We use localhost to force network connection (or local loopback) which uses the client DLL
    con = fdb.connect(
        host='localhost',
        database='C:/non_existent_db.fdb',
        user='SYSDBA',
        password='masterkey',
        charset='WIN1251'
    )
    print("Connected (unexpectedly!)")
    con.close()
except Exception as e:
    print(f"Error type: {type(e)}")
    print(f"Error args: {e.args}")
    print(f"Error str: {str(e)}")
