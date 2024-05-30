from fastapi import FastAPI, HTTPException
from cassandra.cluster import Cluster

class CassandraManager:
    def __init__(self):
        # Kết nối tới Cassandra
        self.cluster = Cluster(['127.0.0.1'])  # Update with your Cassandra server address
        self.session = self.cluster.connect('project')  # Update with your keyspace name

    def execute_query(self, query):
        return self.session.execute(query)

class AppRecommendation:
    cassandra_manager = CassandraManager()

    #Update giá của 1 app bằng id    
    @staticmethod
    def update_app_by_id(price, id):
        try:
            query = "UPDATE mobile_app_store SET price = {} WHERE id = {}".format(price, id)  
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Cập nhật bản ghi thành công"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    #Tìm kiếm 10 app hot nhất theo thể loại nào đó    
    @staticmethod
    def find_10_app_store_by_prime_genre(prime_genre):
        try:
            query = "SELECT * FROM recommend_app_store WHERE prime_genre = '{}' ORDER BY user_rating DESC, rating_count_tot DESC LIMIT 10 ALLOW FILTERING".format(prime_genre)
            result = AppRecommendation.cassandra_manager.execute_query(query)

            apps = []
            for row in result:
                app_data = {
                    "id": row.id,
                    "track_name": row.track_name,
                    "size_bytes": row.size_bytes,
                    "currency": row.currency,
                    "price": row.price,
                    "rating_count_tot": row.rating_count_tot,
                    "rating_count_ver": row.rating_count_ver,
                    "user_rating": row.user_rating,
                    "user_rating_ver": row.user_rating_ver,
                    "ver": row.ver,
                    "cont_rating": row.cont_rating,
                    "prime_genre": row.prime_genre,
                    "sup_devices_num": row.sup_devices_num,
                    "ipadSc_urls_num": getattr(row, 'ipadSc_urls_num', None),
                    "lang_num": row.lang_num,
                    "vpp_lic": row.vpp_lic
                }
                apps.append(app_data)
            return apps
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    # Xóa những app bị đánh giá tệ theo mức độ mà người dùng nhập.    
    @staticmethod
    def delete_app(user_rating):
        try:
            query = "DELETE FROM delete_app WHERE user_rating = {} ".format(user_rating)  
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Xóa bản ghi thành công"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @staticmethod
    def create_emp_table():
        try:
            query = """
            CREATE TABLE IF NOT EXISTS emp (
            emp_id int PRIMARY KEY,
            emp_name text,
            emp_city text,
            emp_sal varint,
            emp_phone varint
        );
        """
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Tạo bảng emp thành công"}
        except Exception as e:
            return {"error": str(e)}
    @staticmethod
    def get_emp_by_id(emp_id):
        try:
            query = "SELECT * FROM emp WHERE emp_id = %s"
            rows = AppRecommendation.cassandra_manager.execute_query(query, (emp_id,))

            emp_info = {}
            for row in rows:
                emp_info[row.emp_id] = {
                    "emp_name": row.emp_name,
                    "emp_city": row.emp_city,
                    "emp_sal": row.emp_sal,
                    "emp_phone": row.emp_phone
                }

            return emp_info
        except Exception as e:
            return {"error": str(e)}
    @staticmethod
    def get_emp_table_info():
        try:
            query = """
            SELECT * from emp;
            """
            rows = AppRecommendation.cassandra_manager.execute_query(query)

            table_info = {}
            for row in rows:
                table_info[row.column_name] = row.type

            return {"emp_table_info": table_info}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def delete_emp_by_id(emp_id):
        try:
            query = "DELETE FROM emp WHERE emp_id = %s"
            AppRecommendation.cassandra_manager.execute_query(query, (emp_id,))
            return {"message": f"Đã xóa nhân viên có ID {emp_id}"}
        except Exception as e:
            return {"error": str(e)}
        
    @staticmethod
    def delete_expensive_apps():
        try:
            query = "DELETE FROM mobile_app_store WHERE price > 50 AND currency = 'USD';"  
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Xóa các bản ghi thành công"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
 
    @staticmethod
    def find_app_by_genre(prime_genre):
        try:
            query = f"SELECT * FROM mobile_app_store WHERE prime_genre = '{prime_genre}' LIMIT 20 ALLOW FILTERING;"
            result = AppRecommendation.cassandra_manager.execute_query(query)
 
            app_details = []
            for row in result:
                app_data = {
                    "id": row.id,
                    "track_name": row.track_name,
                    "size_bytes": row.size_bytes,
                    "currency": row.currency,
                    "price": row.price,
                    "rating_count_tot": row.rating_count_tot,
                    "rating_count_ver": row.rating_count_ver,
                    "user_rating": row.user_rating,
                    "user_rating_ver": row.user_rating_ver,
                    "ver": row.ver,
                    "cont_rating": row.cont_rating,
                    "prime_genre": row.prime_genre,
                    "sup_devices_num": row.sup_devices_num,
                    "ipadSc_urls_num": getattr(row, 'ipadSc_urls_num', None),
                    "lang_num": row.lang_num,
                    "vpp_lic": row.vpp_lic
                }
                app_details.append(app_data)
            return app_details
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    @staticmethod
    def recommend_free_apps():
        try:
            query = "SELECT * FROM recommend_free_app WHERE price = 0 AND currency = 'USD' LIMIT 10 ALLOW FILTERING;"
            result = AppRecommendation.cassandra_manager.execute_query(query)
 
            apps = []
            for row in result:
                app_data = {
                    "id": row.id,
                    "track_name": row.track_name,
                    "size_bytes": row.size_bytes,
                    "currency": row.currency,
                    "price": row.price,
                    "rating_count_tot": row.rating_count_tot,
                    "rating_count_ver": row.rating_count_ver,
                    "user_rating": row.user_rating,
                    "user_rating_ver": row.user_rating_ver,
                    "ver": row.ver,
                    "cont_rating": row.cont_rating,
                    "prime_genre": row.prime_genre,
                    "sup_devices_num": row.sup_devices_num,
                    "ipadSc_urls_num": getattr(row, 'ipadSc_urls_num', None),
                    "lang_num": row.lang_num,
                    "vpp_lic": row.vpp_lic
                }
                apps.append(app_data)
            return apps
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @staticmethod
    def delete_expensive_apps():
        try:
            query = "DELETE FROM mobile_app_store WHERE price > 50 AND currency = 'USD';"  
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Xóa các bản ghi thành công"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @staticmethod
    def find_app_by_primegenre_and_price(prime_genre, low_price, high_price):
        try:
            query = f"SELECT * FROM mobile_app_store WHERE prime_genre = '{prime_genre}' AND price >= {low_price} and price <= {high_price} LIMIT 15 ALLOW FILTERING;"
            result = AppRecommendation.cassandra_manager.execute_query(query)


            app_details = []
            for row in result:
                app_data = {
                    "id": row.id,
                    "track_name": row.track_name,
                    "size_bytes": row.size_bytes,
                    "currency": row.currency,
                    "price": row.price,
                    "rating_count_tot": row.rating_count_tot,
                    "rating_count_ver": row.rating_count_ver,
                    "user_rating": row.user_rating,
                    "user_rating_ver": row.user_rating_ver,
                    "ver": row.ver,
                    "cont_rating": row.cont_rating,
                    "prime_genre": row.prime_genre,
                    "sup_devices_num": row.sup_devices_num,
                    "ipadSc_urls_num": getattr(row, 'ipadSc_urls_num', None),
                    "lang_num": row.lang_num,
                    "vpp_lic": row.vpp_lic
                }
                app_details.append(app_data)
            return app_details
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @staticmethod
    def delete_bad_apps():
        try:
            query = "DELETE FROM no_bad_app WHERE user_rating >= 0 AND user_rating <= 1.5 and currency = 'USD';"  
            AppRecommendation.cassandra_manager.execute_query(query)
            return {"message": "Xóa các bản ghi thành công"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    @staticmethod
    def find_app_by_track_name(track_name):
        try:
            query = f"SELECT * FROM mobile_app_store WHERE track_name = '{track_name}' ALLOW FILTERING;"
            result = AppRecommendation.cassandra_manager.execute_query(query)


            app_details = []
            for row in result:
                app_data = {
                    "id": row.id,
                    "track_name": row.track_name,
                    "size_bytes": row.size_bytes,
                    "currency": row.currency,
                    "price": row.price,
                    "rating_count_tot": row.rating_count_tot,
                    "rating_count_ver": row.rating_count_ver,
                    "user_rating": row.user_rating,
                    "user_rating_ver": row.user_rating_ver,
                    "ver": row.ver,
                    "cont_rating": row.cont_rating,
                    "prime_genre": row.prime_genre,
                    "sup_devices_num": row.sup_devices_num,
                    "ipadSc_urls_num": getattr(row, 'ipadSc_urls_num', None),
                    "lang_num": row.lang_num,
                    "vpp_lic": row.vpp_lic
                }
                app_details.append(app_data)
            return app_details
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))





