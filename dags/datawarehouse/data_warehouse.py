from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursos,
    create_schema,
    create_table,
    get_videos_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task


logger = logging.getLogger(__name__)
table = "yt_api"


@task
def staging_table():
    schema = "staging"
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()
        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_videos_ids(cursor, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cursor, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cursor, conn, schema, row)
                else:
                    insert_rows(cursor, conn, schema, row)

        ids_in_json = {row["video_id"] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table :{e}")
        raise e

    finally:
        if conn and cursor:
            close_conn_cursos(conn, cursor)


@task
def core_table():
    schema = "core"
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()
        create_schema(schema)
        create_table(schema)

        table_ids = get_videos_ids(cursor, schema)

        current_video_ids = set()

        cursor.execute(f"SELECT * FROM staging.{table};")
        rows = cursor.fetchall()

        for row in rows:
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cursor, conn, schema, transformed_row)
            else:

                transformed_row = transform_data(row)
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cursor, conn, schema, transformed_row)
                else:
                    insert_rows(cursor, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids
        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

    except Exception as e:
        logger.exception("run_update failed")
        raise e

    finally:
        if conn and cursor:
            close_conn_cursos(conn, cursor)
