#!/usr/bin/python3
# Author: Jan Kessler
# License: AGPL-3.0

import argparse
import json
import os
import psycopg
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()


def sanitize_chats(main_conn, debug=False):
    """
    Sanitizes the chat table by replacing '\\u0020' with '\\u0000' in the chat column.

    :param main_conn: Connection to the main database
    :param debug: If True, executes SELECT instead of UPDATE and prints results
    """
    if not debug:
        query = """
            UPDATE chat
            SET chat = REPLACE(chat::TEXT, '\\u0000', '\\u0020')::JSONB
            WHERE chat::TEXT LIKE '%\\u0000%'
        """
    else:
        query = """
            SELECT id FROM chat
            WHERE chat::TEXT LIKE '%\\u0000%'
        """
    with main_conn.cursor() as cur:
        cur.execute(query)
        if debug:
            logger.debug(cur.fetchall())
        logger.info(f"Sanitized {cur.rowcount} chats")

def find_referenced_files(main_conn):
    """
    Finds all referenced files in the chat, knowledge, folder, chat_file and knowledge_file tables.

    :param main_conn: Connection to the main database
    :return: A set of referenced file IDs
    """
    query_chat_1 = """
        SELECT DISTINCT jsonb_path_query(chat::jsonb, '$.**.file_id') FROM chat
    """
    query_chat_2 = """
        SELECT DISTINCT jsonb_path_query(chat::jsonb, '$.**.file.id') FROM chat
    """
    query_chat_3 = """
        SELECT DISTINCT jsonb_path_query(chat::jsonb, '$.files.id') FROM chat
    """
    query_chat_file = """
        SELECT DISTINCT file_id FROM chat_file
    """
    query_knowledge = """
        SELECT data FROM knowledge
    """
    query_knowledge_file = """
        SELECT DISTINCT file_id FROM knowledge_file
    """
    query_folder_1 = """
        SELECT DISTINCT jsonb_path_query(data::jsonb, '$.files[*] ? (@.type == "file").file.id') FROM folder
    """
    query_folder_2 = """
        SELECT DISTINCT jsonb_path_query(data::jsonb, '$.files[*] ? (@.type == "file").id') FROM folder
    """
    with main_conn.cursor() as cur:
        # Extract from chats
        cur.execute(query_chat_1)
        files = set(row[0] for row in cur.fetchall())
        cur.execute(query_chat_2)
        files.update(set(row[0] for row in cur.fetchall()))
        cur.execute(query_chat_3)
        files.update(set(row[0] for row in cur.fetchall()))
        # Extract from chat_file
        cur.execute(query_chat_file)
        files.update(set(row[0] for row in cur.fetchall()))
        logger.info(f"Found {len(files)} referenced files in chats.")
        # Extract from knowledge
        cur.execute(query_knowledge)
        for row in cur:
            if row[0]:
                files.update(set(row[0].get('file_ids', [])))
        # Extract from knowledge_file
        cur.execute(query_knowledge_file)
        files.update(set(row[0] for row in cur.fetchall()))
        logger.info(f"Found {len(files)} referenced files in chats and knowledge.")
        # Extract from folders
        cur.execute(query_folder_1)
        files.update(set(row[0] for row in cur.fetchall()))
        cur.execute(query_folder_2)
        files.update(set(row[0] for row in cur.fetchall()))
        logger.info(f"Found {len(files)} referenced files in chats, knowledge and folders.")
        logger.debug(f"Referenced files: {files}")
        return files

def find_unused_files_db(main_conn, referenced_files):
    """
    Identifies unused files in the main database by comparing all files with referenced files.

    :param main_conn: Connection to the main database
    :param referenced_files: Set of referenced file IDs
    :return: A set of unused file IDs
    """
    query = """
        SELECT id FROM file
    """
    with main_conn.cursor() as cur:
        cur.execute(query)
        all_files = set(row[0] for row in cur.fetchall())
        logger.info(f"Found {len(all_files)} files in main DB.")
        logger.debug(f"All files: {all_files}")
        unused_files = all_files - referenced_files
        logger.info(f"Found {len(unused_files)} unused files in main DB.")
        logger.debug(f"Unused files: {unused_files}")
        return unused_files

def find_referenced_collections(main_conn):
    """
    Finds all referenced collections in the chat, file, and memory tables.

    :param main_conn: Connection to the main database
    :param vector_conn: Connection to the vector database
    :return: A set of referenced collection names
    """
    query_chat_1 = """
        SELECT DISTINCT jsonb_path_query(chat::jsonb, '$.**.collection_name') FROM chat
    """
    query_chat_2 = """
        SELECT DISTINCT jsonb_path_query(chat::jsonb, '$.files[*] ? (@.type == "collection").id') FROM chat
    """
    query_files = """
        SELECT DISTINCT jsonb_path_query(meta::jsonb, '$.collection_name') FROM file
    """
    query_memory = """
        SELECT DISTINCT user_id FROM memory
    """

    with main_conn.cursor() as cur:
        cur.execute(query_chat_1)
        collections = set(row[0] for row in cur.fetchall())
        cur.execute(query_chat_2)
        collections.update(set(row[0] for row in cur.fetchall()))
        logger.info(f"Found {len(collections)} referenced collections in chats.")
        cur.execute(query_files)
        collections.update(set(row[0] for row in cur.fetchall()))
        logger.info(f"Found {len(collections)} referenced collections in chats and files.")
        cur.execute(query_memory)
        collections.update(set(f"user-memory-{row[0]}" for row in cur.fetchall()))
        logger.info(f"Found {len(collections)} referenced collections in chats, files and memory.")
        logger.debug(f"Referenced Collections: {collections}")
        return collections

def find_unused_collections(vector_conn, referenced_collections):
    """
    Identifies unused collections in the vector database by comparing all collections with referenced collections.

    :param vector_conn: Connection to the vector database
    :param referenced_collections: Set of referenced collection names
    :return: A set of unused collection names
    """
    query = """
        SELECT DISTINCT collection_name FROM document_chunk
    """
    with vector_conn.cursor() as cur:
        cur.execute(query)
        all_collections = set(row[0] for row in cur.fetchall())
        logger.info(f"Found {len(all_collections)} collections in vector DB.")
        logger.debug(f"All Collections: {all_collections}")
        unused_collections = all_collections - referenced_collections
        logger.info(f"Found {len(unused_collections)} unused collections in vector DB.")
        logger.debug(f"Unused Collections: {unused_collections}")
        return unused_collections

def get_filenames_by_ids(main_conn, file_ids):
    """
    Retrieves a set of filenames from the file table given a set of file IDs using the path column.

    :param main_conn: Connection to the main database
    :param file_ids: Set of file IDs
    :return: A set of filenames
    """
    query = """
        SELECT DISTINCT path FROM file WHERE id = ANY(%s)
    """
    if not file_ids:
        return set()

    with main_conn.cursor() as cur:
        cur.execute(query, [list(file_ids)])
        filenames = set(os.path.basename(row[0]) for row in cur.fetchall())
        logger.info(f"Retrieved {len(filenames)} filenames for the given file IDs.")
        logger.debug(f"Filenames: {filenames}")
        return filenames

def find_unused_filenames_fs(referenced_filenames, uploads_dir):
    """
    Identifies unused files on the filesystem by comparing files in the database with files in the uploads directory.

    :param referenced_filenames: Set of filenames present in the file table
    :param uploads_dir: Path to the uploads directory
    :return: A set of filenames to be deleted from the filesystem
    """
    try:
        filenames = set(os.listdir(uploads_dir))
        logger.info(f"Found {len(filenames)} files in uploads directory.")
        logger.debug(f"Files in uploads directory: {filenames}")
    except Exception as e:
        logger.error(f"Error listing files in uploads directory: {str(e)}")
        raise
    unused_filenames = filenames - referenced_filenames
    logger.info(f"Found {len(unused_filenames)} unused files in uploads directory.")
    logger.debug(f"Unused files in uploads directory: {unused_filenames}")
    return unused_filenames

def cleanup_chats(main_conn, days, debug=False):
    """
    Cleans up old chats from the main database.
    Also removes related entries from chat_file.

    :param main_conn: Connection to the main database
    :param days: Number of days to keep chats
    :param debug: If True, executes SELECT instead of DELETE and prints results
    """
    if not debug:
        query_chats = """
            DELETE FROM chat
            WHERE NOT archived AND created_at < %s
            RETURNING id
        """
        query_files = """
            DELETE FROM chat_file
            WHERE chat_id = ANY(%s)
        """
    else:
        query_chats = """
            SELECT id FROM chat
            WHERE NOT archived AND created_at < %s
        """
        query_files = """
            SELECT id FROM chat_file
            WHERE chat_id = ANY(%s)
        """
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
    with main_conn.cursor() as cur:
        cur.execute(query_chats, (cutoff,))
        chat_ids = [row[0] for row in cur.fetchall()]
        if debug:
            logger.debug(cur.fetchall())
        logger.info(f"Deleted {cur.rowcount} chats")
        cur.execute(query_files, [chat_ids])
        if debug:
            logger.debug(cur.fetchall())
        logger.info(f"Deleted {cur.rowcount} chat_file entries.")

def cleanup_files_db(main_conn, unused_files, debug=False):
    """
    Deletes unused files from the files table in the main database.

    :param main_conn: Connection to the main database
    :param unused_files: Set of unused file IDs
    :param debug: If True, executes SELECT instead of DELETE and prints results
    """
    if not debug:
        query = """
            DELETE FROM file
            WHERE id = ANY(%s)
        """
    else:
        query = """
            SELECT id FROM file
            WHERE id = ANY(%s)
        """
    with main_conn.cursor() as cur:
        cur.execute(query, [list(unused_files)])
        if debug:
            logger.debug(cur.fetchall())
        logger.info(f"Deleted {cur.rowcount} files from DB")

def cleanup_collections(vector_conn, unused_collections, debug=False):
    """
    Deletes unused collections from the vector database.

    :param vector_conn: Connection to the vector database
    :param unused_collections: Set of unused collection names
    :param debug: If True, executes SELECT instead of DELETE and prints results
    """
    if not debug:
        query = """
            DELETE FROM document_chunk
            WHERE collection_name = ANY(%s)
        """
    else:
        query = """
            SELECT collection_name FROM document_chunk
            WHERE collection_name = ANY(%s)
        """
    with vector_conn.cursor() as cur:
        cur.execute(query, [list(unused_collections)])
        if debug:
            logger.debug(cur.fetchall())
        logger.info(f"Deleted {cur.rowcount} collections")

def cleanup_files_fs(unused_filenames, uploads_dir, dry_run=False):
    """
    Deletes unused files from the uploads directory.

    :param unused_filenames: Set of filenames to be deleted
    :param uploads_dir: Path to the uploads directory
    :param dry_run: If True, performs a dry run without deleting any files
    """
    num_deleted_files = 0
    for filename in unused_filenames:
        file_path = os.path.join(uploads_dir, filename)
        if not dry_run:
            try:
                os.remove(file_path)
                logger.debug(f"Deleted file: {file_path}")
                num_deleted_files += 1
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {str(e)}")
        else:
            logger.debug(f"Deleted file: {file_path}")
            num_deleted_files += 1
    logger.info(f"Deleted {num_deleted_files} files from filesystem")

def main():
    """
    Main function to execute the cleanup script.

    Parses command line arguments, establishes database connections, and performs cleanup operations.
    """
    parser = argparse.ArgumentParser(description='Open WebUI Cleanup Script')
    parser.add_argument('--main-db-url', required=True)
    parser.add_argument('--vector-db-url', required=True)
    parser.add_argument('--uploads-dir', required=True)
    parser.add_argument('--keep-days', type=int, required=True)
    parser.add_argument('--dry-run', action='store_true', default=False)
    parser.add_argument('--verbose', action='store_true', default=False)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--sanitize', action='store_true', default=False)
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        args.dry_run = True

    main_conn = psycopg.connect(args.main_db_url)
    vector_conn = psycopg.connect(args.vector_db_url)

    try:
        # Step 1: Sanitize chats
        if args.sanitize:
            sanitize_chats(main_conn, args.debug)

        # Step 2: Cleanup old chats
        cleanup_chats(main_conn, args.keep_days, args.debug)

        # Step 3: Delete unused files from DB
        referenced_files = find_referenced_files(main_conn)
        unused_files = find_unused_files_db(main_conn, referenced_files)
        cleanup_files_db(main_conn, unused_files, args.debug)

        # Step 4: Delete unused collections from vector DB
        referenced_collections = find_referenced_collections(main_conn)
        unused_collections = find_unused_collections(vector_conn, referenced_collections)
        cleanup_collections(vector_conn, unused_collections, args.debug)

        # Step 5: Delete unused files from filesystem
        referenced_filenames = get_filenames_by_ids(main_conn, referenced_files)
        unused_filenames_fs = find_unused_filenames_fs(referenced_filenames, args.uploads_dir)
        cleanup_files_fs(unused_filenames_fs, args.uploads_dir, args.dry_run)

        if not args.dry_run:
            main_conn.commit()
            vector_conn.commit()

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        main_conn.close()
        vector_conn.close()


if __name__ == '__main__':
    main()
