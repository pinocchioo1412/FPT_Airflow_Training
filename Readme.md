# Các bước cài đặt và sử dụng project 
## Bước 1 : clone repo này về máy
## Bước 2 : Khởi tạo các container : 
Mở folder ứng với project này, chọn new terminal và chạy lệnh : docker-compose up --build
Sau đó mở cổng 8080 và đảm bảo thấy giao diện airflow webserver : 
<img width="1836" height="925" alt="image" src="https://github.com/user-attachments/assets/0a1197aa-2bac-454b-a53e-3416e5cc7741" />
## Bước 3 : đảm bảo airflow connect với postgreSQL
có thể chạy lệnh sau để kiểm tra xem có kết nối được với postgresql không : 
docker exec -it fpt_airflow_training-postgres-1 psql -U airflow -d airflow
## Bước 4 : Trigger dag etl_postgre_test2 
Sau khi trigger dag, có thể kiểm tra graph để xem luồng hoạt động như hình bên dưới :
graph dag :
![alt text](image-7.png)

## Bước 5 : check log 

log dag create table : 
![alt text](image-8.png)

log dag stage_songs : 
![alt text](image-9.png)

log dag stage_events : 
![alt text](image-10.png)

log dag load_songplays_fact_table : 
![alt text](image-11.png)

log dag load_user_dim_table : 
![alt text](image-12.png)

log dag load_song_dim_table : 
![alt text](image-13.png)

log dag load_artist_dim_table : 
![alt text](image-14.png)

log dag load_time_dim_table : 
![alt text](image-15.png)

log dag run_quality_check : 
![alt text](image-16.png)

## Bước 6 : kiểm tra data trong các bảng : 
bảng songs
![alt text](image.png)

bảng users

![alt text](image-1.png)

bảng artist

![alt text](image-2.png)

bảng time

![alt text](image-3.png)

bảng songplays

![alt text](image-4.png)

bảng staging_events: 

![alt text](image-5.png)

bảng staging_songs:

![alt text](image-6.png)
