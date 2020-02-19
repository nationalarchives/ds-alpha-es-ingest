def lettercodes_query() -> str:
    return f"""
        SELECT DISTINCT tbl_lettercode.letter_code, tbl_lettercode.lettercode_title as title
        FROM tbl_lettercode
        """


def piece_query(lettercode: str) -> str:
    """
    Return a query string for fetching pieces for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
            SELECT DISTINCT tbl_lettercode.letter_code, tbl_Division.division_no, 
            tbl_class.class_no, tbl_class.subclass_no,
            tbl_header.class_hdr_no, tbl_subheader.subheader_no,
            tbl_piece.piece_ref, tbl_piece.first_date, tbl_piece.last_date, tbl_piece.piece_scope AS title
            FROM tbl_lettercode
            INNER JOIN tbl_class on tbl_class.lettercode_id = tbl_lettercode.lettercode_id
            INNER JOIN tbl_piece on tbl_piece.class_id = tbl_class.class_id
            LEFT JOIN tbl_header on tbl_header.header_id = tbl_piece.header_id
            LEFT JOIN tbl_subheader on tbl_subheader.subheader_id = tbl_piece.subheader_id
            LEFT JOIN tbl_Division on tbl_Division.Division_ID = tbl_class.division_id
            WHERE tbl_lettercode.letter_code = '{lettercode}'
            """


def item_query(lettercode: str) -> str:
    """
    Return a query string for fetching items for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
    SELECT DISTINCT tbl_lettercode.letter_code, tbl_class.class_no, tbl_class.subclass_no,
    tbl_piece.piece_ref, tbl_item.item_ref, tbl_Division.division_no, tbl_header.class_hdr_no, 
    tbl_subheader.subheader_no, tbl_item.first_date, tbl_item.last_date, tbl_item.item_scope as title
    FROM tbl_lettercode
    INNER JOIN tbl_class on tbl_class.lettercode_id = tbl_lettercode.lettercode_id
    INNER JOIN tbl_piece on tbl_piece.class_id = tbl_class.class_id
    INNER JOIN tbl_item on tbl_item.piece_id = tbl_piece.piece_id
    LEFT JOIN tbl_header on tbl_header.header_id = tbl_piece.header_id
    LEFT JOIN tbl_subheader on tbl_subheader.subheader_id = tbl_piece.subheader_id
    LEFT JOIN tbl_Division on tbl_Division.Division_ID = tbl_class.division_id
    WHERE tbl_lettercode.letter_code = '{lettercode}'
    """


def series_query(lettercode: str) -> str:
    """
    Return a query string for fetching series for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
    SELECT DISTINCT tbl_lettercode.letter_code, tbl_Division.division_no,
    tbl_class.class_no, tbl_class.subclass_no, tbl_class.first_date, tbl_class.last_date, tbl_class.class_title as title
    FROM tbl_lettercode
    INNER JOIN tbl_class on tbl_class.lettercode_id = tbl_lettercode.lettercode_id
    LEFT JOIN tbl_Division on tbl_Division.Division_ID = tbl_class.division_id
    WHERE tbl_lettercode.letter_code = '{lettercode}'
    """


def subseries_query(lettercode: str) -> str:
    """
    Return a query string for fetching subseries for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
    SELECT tbl_lettercode.letter_code, tbl_Division.division_no, tbl_class.class_no, 
    tbl_class.subclass_no, tbl_header.class_hdr_no, tbl_header.header_title AS title
    FROM tbl_lettercode
    INNER JOIN tbl_class on tbl_class.lettercode_id = tbl_lettercode.lettercode_id
    LEFT JOIN tbl_Division on tbl_Division.Division_ID = tbl_class.division_id
    INNER JOIN tbl_header on tbl_header.class_id = tbl_class.class_id
    WHERE tbl_lettercode.letter_code = '{lettercode}'"""


def subsubseries_query(lettercode: str) -> str:
    """
    Return a query string for fetching pieces for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
    SELECT tbl_lettercode.letter_code, tbl_Division.division_no,
    tbl_class.class_no, 
    tbl_class.subclass_no, tbl_header.class_hdr_no, tbl_subheader.subheader_no, tbl_subheader.subheader_title as title
    FROM tbl_lettercode
    INNER JOIN tbl_class on tbl_class.lettercode_id = tbl_lettercode.lettercode_id
    LEFT JOIN tbl_Division on tbl_Division.Division_ID = tbl_class.division_id
    INNER JOIN tbl_header on tbl_header.class_id = tbl_class.class_id
    INNER JOIN tbl_subheader on tbl_subheader.header_id = tbl_header.header_id
    WHERE tbl_lettercode.letter_code = '{lettercode}'        """


def division_query(lettercode: str) -> str:
    """
    Return a query string for fetching divisions for a lettercode

    :param lettercode:
    :return:
    """
    return f"""
    SELECT tbl_lettercode.letter_code, tbl_Division.division_no, tbl_Division.Division_Title as title
    FROM tbl_lettercode
    INNER JOIN tbl_Division ON tbl_lettercode.lettercode_id = tbl_Division.lettercode_id
    WHERE tbl_lettercode.letter_code = '{lettercode}'
    """

