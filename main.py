import psycopg2
from pprint import pprint


def _find_client_id(connect, client_id):
    with connect.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM client
             WHERE client_id = %s;
            """,
            (client_id,)
        )
        result = cur.fetchone()[0]
    return result


def _find_email(connect, email):
    with connect.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM client
             WHERE email = %s;
            """,
            (email,)
        )
        result = cur.fetchone()[0]
    return result


def _find_phone(connect, phone_number):
    with connect.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM phone
             WHERE phone_number = %s;
            """,
            (phone_number,)
        )
        result = cur.fetchone()[0]
    return result


def _get_client_phones_list(connect, client_id):
    with connect.cursor() as cur:
        cur.execute(
            """
            SELECT phone_number
              FROM phone
             WHERE client_id = %s;
            """,
            (client_id,)
        )
        phones_list = [number[0] for number in cur.fetchall()]
    return phones_list


def create_db(connect):
    with connect.cursor() as cur:
        cur.execute(
            """
            DROP TABLE phone;
            DROP TABLE client;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client (
                client_id  SERIAL      PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name  VARCHAR(40) NOT NULL,
                email      VARCHAR(40) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS phone (
                client_id    INTEGER     REFERENCES client (client_id),
                phone_number VARCHAR(12)
            );
            """
        )
        connect.commit()
    return 'База данных успешно создана.'


def add_client(connect, first_name, last_name, email, phone_number=None):
    with connect.cursor() as cur:
        check1 = _find_email(connect, email)
        if not check1:
            if phone_number:
                check2 = _find_phone(connect, phone_number)
                if not check2:
                    cur.execute(
                        """
                        INSERT INTO client (first_name, last_name, email)
                        VALUES (%s, %s, %s)
                        RETURNING client_id;
                        """,
                        (first_name, last_name, email)
                    )
                    client_id = cur.fetchone()
                    cur.execute(
                        """
                        INSERT INTO phone (client_id, phone_number)
                        VALUES (%s, %s);
                        """,
                        (client_id, phone_number)
                    )
                    connect.commit()
                    return 'Клиент успешно добавлен.'
                return 'Такой номер телефона уже существует.'
            else:
                cur.execute(
                    """
                    INSERT INTO client (first_name, last_name, email)
                    VALUES (%s, %s, %s);
                    """,
                    (first_name, last_name, email)
                )
                connect.commit()
                return 'Клиент успешно добавлен.'
        return 'Такой email уже существует.'


def change_client(connect, client_id, first_name=None, last_name=None, email=None, phone_number=None):
    with connect.cursor() as cur:
        check1 = _find_client_id(connect, client_id)
        if check1:
            if first_name:
                cur.execute(
                    """
                    UPDATE client
                       SET first_name = %s
                     WHERE client_id = %s;
                    """,
                    (first_name, client_id)
                )
            if last_name:
                cur.execute(
                    """
                    UPDATE client
                       SET last_name = %s
                     WHERE client_id = %s;
                    """,
                    (last_name, client_id)
                )
            if email:
                cur.execute(
                    """
                    UPDATE client
                       SET email = %s
                     WHERE client_id = %s;
                    """,
                    (email, client_id)
                )
            if phone_number:
                check2 = _find_phone(connect, phone_number)
                if not check2:
                    phones_list = _get_client_phones_list(connect, client_id)
                    phones_count = len(phones_list)
                    if phones_count == 0:
                        cur.execute(
                            """
                            INSERT INTO phone (client_id, phone_number)
                            VALUES (%s, %s);
                            """,
                            (client_id, phone_number)
                        )
                        connect.commit()
                        return 'Данные клиента успешно обновлены.'
                    elif phones_count == 1:
                        cur.execute(
                            """
                            UPDATE phone
                               SET phone_number = %s
                             WHERE client_id = %s;
                            """,
                            (phone_number, client_id)
                        )
                        connect.commit()
                        return 'Данные клиента успешно обновлены.'
                    else:
                        replacement_phone = input('Укажите номер телефона, который нужно заменить: ').strip()
                        while replacement_phone not in phones_list:
                            print('Такого номера телефона не существует.')
                            replacement_phone = input('Укажите номер телефона, который нужно заменить: ').strip()
                        cur.execute(
                            """
                            UPDATE phone
                               SET phone_number = %s
                             WHERE client_id = %s
                               AND phone_number = %s;
                            """,
                            (phone_number, client_id, replacement_phone)
                        )
                        connect.commit()
                        return 'Данные клиента успешно обновлены.'
                return 'Такой номер телефона уже существует.'
            connect.commit()
            return 'Данные клиента успешно обновлены.'
        return 'Клиента с таким ID не существует.'


def delete_client(connect, client_id):
    with connect.cursor() as cur:
        check = _find_client_id(connect, client_id)
        if check:
            cur.execute(
                """
                DELETE FROM phone
                WHERE client_id = %s;
                
                DELETE FROM client
                WHERE client_id = %s;
                """,
                (client_id, client_id)
            )
            connect.commit()
            return 'Клиент успешно удалён.'
        return 'Клиента с таким ID не существует.'


def add_phone(connect, client_id, phone_number):
    with connect.cursor() as cur:
        check1 = _find_client_id(connect, client_id)
        if check1:
            check2 = _find_phone(connect, phone_number)
            if not check2:
                cur.execute(
                    """
                    INSERT INTO phone (client_id, phone_number)
                    VALUES (%s, %s);
                    """,
                    (client_id, phone_number)
                )
                connect.commit()
                return 'Номер телефона успешно добавлен.'
            return 'Такой номер телефона уже существует.'
        return 'Клиента с таким ID не существует.'


def delete_phone(connect, phone_number):
    with connect.cursor() as cur:
        check = _find_phone(connect, phone_number)
        if check:
            cur.execute(
                """
                DELETE FROM phone
                 WHERE phone_number = %s;
                """,
                (phone_number,)
            )
            connect.commit()
            return 'Номер телефона успешно удалён.'
        return 'Такого номера телефона не существует.'


def delete_all_phones(connect, client_id):
    with connect.cursor() as cur:
        check = _find_client_id(connect, client_id)
        if check:
            cur.execute(
                """
                DELETE FROM phone
                 WHERE client_id = %s;
                """,
                (client_id,)
            )
            connect.commit()
            return 'Все номера телефонов успешно удалены.'
        return 'Клиента с таким ID не существует.'


def find_client(connect, first_name='%%', last_name='%%', email='%%', phone_number=None):
    with connect.cursor() as cur:
        if phone_number:
            cur.execute(
                """
                SELECT c.client_id, first_name, last_name, email, phone_number
                  FROM client AS c
                       LEFT JOIN phone AS p
                       ON c.client_id = p.client_id
                 WHERE first_name ILIKE %s
                   AND last_name ILIKE %s
                   AND email ILIKE %s
                   AND phone_number LIKE %s;
                """,
                (first_name, last_name, email, phone_number)
            )
        else:
            cur.execute(
                """
                SELECT c.client_id, first_name, last_name, email, phone_number
                  FROM client AS c
                       LEFT JOIN phone AS p
                       ON c.client_id = p.client_id
                 WHERE first_name ILIKE %s
                   AND last_name ILIKE %s
                   AND email ILIKE %s;
                """,
                (first_name, last_name, email)
            )
        result = cur.fetchall()
        if result:
            pprint(result)
        else:
            print('Ничего не найдено.')


if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:

        print(create_db(conn))

        print(add_client(conn, 'Антон', 'Богданов', 'a_bogdanov@mail.ru', '+79505000024'))
        print(add_client(conn, 'Мария', 'Пшеничникова', 'p_mariya@bk.ru', '+79505000134'))
        print(add_client(conn, 'Николай', 'Коробов', 'iron_man@mail.ru', '+79100000036'))
        print(add_client(conn, 'Сергей', 'Кузнецов', 'kuznecov@yandex.ru', '+79600000057'))
        print(add_client(conn, 'Антон', 'Озеров', 'ozerov@gmail.com'))

        print(add_phone(conn, 2, '+79005006000'))

        print(change_client(conn, 5, phone_number='+79900000370'))
        print(change_client(conn, 2, last_name='Коваленко', email='mir@yandex.ru', phone_number='+79501505050'))
        print(change_client(conn, 2, email='p_mariya@bk.ru'))
        print(change_client(conn, 2, first_name='Мария', last_name='Пшеничникова'))

        print(delete_phone(conn, '+79501505050'))
        print(delete_phone(conn, '+79900000370'))

        print(delete_all_phones(conn, 2))

        print(delete_client(conn, 4))

        find_client(conn, first_name='Антон')
        find_client(conn, last_name='Коробов')
        find_client(conn, email='ozerov@gmail.com')
        find_client(conn, phone_number='+79100000036')
        find_client(conn, first_name='Антон', email='a_bogdanov@mail.ru')

    conn.close()
