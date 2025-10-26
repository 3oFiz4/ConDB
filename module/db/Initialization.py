from supabase import create_client as db_init_client

class Initialization:
    def __init__(self, db_url, db_pass, table, local_dir):
        self.url = db_url # Database URL
        self.password = db_pass # Database Password
        self.table = table # Database table, or the storage 
        self.local_dir = local_dir # Local directory
        self.session = self._connect_remote() 

    def _connect_remote(self):
        Client = db_init_client(self.url, self.password)
        return Client

    def new_data(self, **filters):
        return DataOperation(connection=self, filters=filters)
