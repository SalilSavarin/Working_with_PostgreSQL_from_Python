import psycopg2
from password import password_postgresql as password



connect_db = psycopg2.connect(database='userdb', user='postgres', password=password)

#Функция, создающая структуру БД (таблицы) 
def create_db(conn): 
    with conn.cursor() as cur: 
        cur.execute(
        ''' 
        CREATE TABLE Users 
        (
        user_id SERIAL PRIMARY KEY, 
        first_name VARCHAR (90), 
        second_name VARCHAR (90), 
        email VARCHAR(90)
        ); 
        ''') 
        cur.execute(
            ''' 
        CREATE TABLE user_phone 
        (
        id_phone SERIAL PRIMARY KEY,
        id_user SERIAL REFERENCES Users(user_id), 
        phone TEXT 
        ); 
        ''')
        conn.commit() 

#Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    conn = connect_db
    with conn.cursor() as cur:
        cur.execute(
        '''
        SELECT max(user_id) FROM users;
        ''')
        id = cur.fetchall()
        for x in id:
            #ПЕРВЫЙ пользователь с номером телефона
            if x[0] == None and phones != None:
                values = ({'first_name': first_name, 'last_name': last_name, 'email': email})
                cur.execute(
                '''
                INSERT INTO users
                VALUES
                (
                1, %(first_name)s, %(last_name)s, %(email)s
                ) ;
                ''', values)   
                values_with_phone = ({'id_phone': '1', 'id_user': '1', 'phone': phones})
                cur.execute(
                '''
                INSERT INTO user_phone
                VALUES
                (
                %(id_phone)s, %(id_user)s, %(phone)s
                )
                ''', values_with_phone)
                conn.commit()
            #ПЕРВЫЙ пользователь без номера телефона    
            elif x[0] == None and phones == None:
                values = ({'first_name': first_name, 'last_name': last_name, 'email': email})
                cur.execute(
                '''
                INSERT INTO users
                VALUES
                (
                1, %(first_name)s, %(last_name)s, %(email)s 
                ) ;
                ''', values)
                cur.execute()
                conn.commit()
            #Новый пользователь с номером телефона
            elif x[0] != None and phones != None:
                new_id = str(x[-1]+1)
                values = ({'user_id': new_id, 'first_name': first_name, 'last_name': last_name, 'email': email})
                cur.execute(
                '''
                INSERT INTO users
                VALUES
                (
                %(user_id)s, %(first_name)s, %(last_name)s, %(email)s 
                );
                ''', values)
                cur.execute(
                '''
                SELECT max(id_phone) FROM user_phone;
                ''')
                new_id_phone = cur.fetchall()
                for y in new_id_phone:
                    id_phone = str(y[-1]+1)
                    values_with_phone = ({'id_phone': id_phone, 'id_user': new_id, 'phone': phones})
                    cur.execute(
                    '''
                    INSERT INTO user_phone
                    VALUES
                    (
                    %(id_phone)s, %(id_user)s, %(phone)s
                    )
                    ''', values_with_phone)
                conn.commit()
            #Новый пользователь без номером телефона
            elif x[0] != None and phones == None:
                new_id = str(x[-1]+1)
                values = ({'user_id': new_id, 'first_name': first_name, 'last_name': last_name, 'email': email})
                cur.execute(
                '''
                INSERT INTO users
                VALUES
                (
                %(user_id)s, %(first_name)s, %(last_name)s, %(email)s 
                );
                ''', values)
                conn.commit()

#Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    conn = connect_db
    with conn.cursor() as cur:
        values = ({'client_id': client_id})
        cur.execute(
        '''
        SELECT phone FROM user_phone up 
        JOIN users u ON u.user_id = up.id_phone 
        WHERE u.user_id = %(client_id)s;
        ''', values
        )       
        number = cur.fetchall()
        if number == []:
            cur.execute('''
            SELECT MAX(id_phone) FROM user_phone ;
            ''')
            max_id_phone = cur.fetchall()
            for x in max_id_phone:
                new_id_phone = str(x[0] + 1)
            values_without_phone = ({'new_id_phone': new_id_phone, 'client_id': client_id, 'phone': phone})    
            cur.execute('''
            INSERT INTO user_phone(id_phone, id_user, phone)
            VALUES (%(new_id_phone)s, %(client_id)s, %(phone)s) ;
            ''', values_without_phone
            )
            conn.commit()
        elif number != []:
            values_for_search_phone = ({'client_id': client_id})
            cur.execute('''
            SELECT phone FROM user_phone u
            WHERE u.id_user = %(client_id)s ;
            ''',values_for_search_phone)
            old_number = cur.fetchall()
            for x in old_number:
                update_number = x[0] + ' ' + phone
            values_with_phone = ({'client_id': client_id, 'phone': update_number})    
            cur.execute('''
            UPDATE user_phone
            SET phone = %(phone)s
            WHERE id_user = %(client_id)s ;
            ''',values_with_phone)
            conn.commit()

#Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, new_phone=None, old_phone=None):
    conn = connect_db

    def change_first_name(client_id, first_name):
        with conn.cursor() as cur:
            values = ({'client_id': client_id, 'first_name': first_name})
            cur.execute('''
            UPDATE users
            SET first_name = %(first_name)s
            WHERE user_id = %(client_id)s ;
            ''',values)
            conn.commit()

    def change_last_name(client_id, last_name):
        with conn.cursor() as cur:
            values = ({'client_id': client_id, 'last_name': last_name})
            cur.execute('''
            UPDATE users
            SET second_name = %(last_name)s
            WHERE user_id = %(client_id)s ;
            ''',values)
            conn.commit()

    def change_email(client_id, email):
        with conn.cursor() as cur:
            values = ({'client_id': client_id, 'email': email})
            cur.execute('''
            UPDATE users
            SET email = %(email)s
            WHERE user_id = %(client_id)s ;
            ''',values)
            conn.commit()

    def change_phone(client_id, new_phone, old_phone):
        with conn.cursor() as cur:
            values_for_search_phone = ({'client_id': client_id})
            cur.execute('''
            SELECT phone FROM user_phone up
            WHERE id_user = %(client_id)s ;
            ''',values_for_search_phone)
            phone = cur.fetchall()
            phone_for_update = None
            for x in phone:
                for y in x:
                    phone_list = y.split()
                    print(phone_list)
                if old_phone in phone_list:
                    phone_list.remove(old_phone)
                    phone_list.append(new_phone)
            phone_for_update = (' ').join(phone_list)
            values_for_update = ({'client_id': client_id, 'phone': phone_for_update})
            cur.execute('''
            UPDATE user_phone
            SET phone = %(phone)s
            WHERE id_user = %(client_id)s ;
            ''',values_for_update)
            conn.commit()

    if first_name != None:
        change_first_name(client_id, first_name)   
    elif last_name != None:
        change_last_name(client_id, last_name)
    elif email != None:
        change_email(client_id, email)
    elif old_phone != None and new_phone != None:
        change_phone(client_id, new_phone, old_phone)

#Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    conn = connect_db
    with conn.cursor() as cur:
        new_list_num = []
        number_str = ''
        values_for_search_phone = ({'client_id': client_id})
        cur.execute('''
        SELECT phone FROM user_phone u
        WHERE u.id_user = %(client_id)s ;
        ''',values_for_search_phone)
        old_number = cur.fetchall()
        phone_update = ''
        for phone_old in old_number[0]:
            phone_list = phone_old.split(' ')
        for x in phone_list:
            if x == phone:
                phone_list.remove(x)
                if len(phone_list) < 1:
                    values_for_del = ({'client_id': client_id})
                    cur.execute('''
                    DELETE FROM user_phone
                    WHERE id_user = %(client_id)s
                    ''',values_for_del)
                    conn.commit()    
                elif len(phone_list) >= 1:
                    for y in phone_list:
                        phone_update += y
                        str_num = phone_update.split('+')
                    for z in str_num:
                        if z != '':
                            z = '+' + z
                            new_list_num.append(z)
            for number in new_list_num:
                number_str += number
                number_str += ' '
            strip_number_str = number_str.strip()
            if len(strip_number_str) >= 1:
                values_for_update = ({'client_id': client_id, 'phone': strip_number_str})
                cur.execute('''
                UPDATE user_phone
                SET phone = %(phone)s
                WHERE id_user = %(client_id)s;
                ''',values_for_update)
                conn.commit()
            elif len(phone_list) < 1:
                values_for_del = ({'client_id': client_id})
                cur.execute('''
                DELETE FROM user_phone
                WHERE id_user = %(client_id)s
                ''',values_for_del)
                conn.commit()

#Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    conn = connect_db
    with conn.cursor() as cur:
        values_for_del = ({'client_id': client_id})
        cur.execute('''
        DELETE FROM user_phone
        WHERE id_user = %(client_id)s;
        ''',values_for_del)
        cur.execute('''
        DELETE FROM users
        WHERE user_id = %(client_id)s;
        ''',values_for_del)
    conn.commit()

#Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def search_client(conn, first_name=None, last_name=None, email=None, phone=None):
    conn = connect_db

    def search_first_name(first_name):
        with conn.cursor() as cur:
            values_for_search = ({'first_name': first_name})
            cur.execute('''
            SELECT user_id, first_name, second_name, email, up.phone FROM users u
            JOIN user_phone up ON up.id_user = u.user_id
            WHERE first_name = %(first_name)s;
            ''',values_for_search)
            res = cur.fetchall()
        print(res)

    def search_second_name(last_name):
        with conn.cursor() as cur:
            values_for_search = ({'last_name': last_name})
            cur.execute('''
            SELECT user_id, first_name, second_name, email, up.phone FROM users u
            JOIN user_phone up ON up.id_user = u.user_id
            WHERE second_name = %(last_name)s;
            ''',values_for_search)
            res = cur.fetchall()
        print(res)

    def search_email(email):    
        with conn.cursor() as cur:
            values_for_search = ({'email': email})
            cur.execute('''SELECT user_id, first_name, second_name, email, up.phone FROM users u
            JOIN user_phone up ON up.id_user = u.user_id
            WHERE email = %(email)s;
            ''',values_for_search)
            res = cur.fetchall()
        print(res)

    def search_phone(phone):
        phone_for_search = '%'+ phone + '%'
        with conn.cursor() as cur:
            values_for_search = ({'phone': phone_for_search})
            cur.execute('''SELECT user_id, first_name, second_name, email, up.phone FROM users u
            JOIN user_phone up ON up.id_user = u.user_id
            WHERE phone LIKE %(phone)s 
            ''',values_for_search)
            res = cur.fetchall()
        print(res)


    if first_name != None:
        search_first_name(first_name=first_name)
    elif last_name != None:
        search_second_name(last_name=last_name)
    elif email != None:
        search_email(email=email)
    elif phone != None:
        search_phone(phone=phone)
              


# search_client(connect_db,phone='+79217889051')
# add_client(connect_db,'Bruce','Wayne','bway@dc.com')
# add_phone(connect_db,'3','+79998887766')
# add_phone(connect_db,'3','+79008007060')
# delete_phone(connect_db, '3', '+79998887766')
# delete_phone(connect_db, '3', '+79008007060')
# delete_client(connect_db, '4')
# search_client(connect_db,phone='+79007001122')
change_client(connect_db,client_id='3',new_phone='+79098087060', old_phone='+79998887766')


connect_db.close()
