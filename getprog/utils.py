import os

PAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "current_page.txt")


def write_page_to_file(page: int) -> None:
    """
    Writes the given page number to a file.

    :param page: The page number to write to the file.
    """
    with open(PAGE_FILE, "w") as file:
        file.write(str(page))


def read_page_from_file() -> int:
    """
    Reads the page number from a file.

    :return: The page number read from the file.
    :raises FileNotFoundError: If the file does not exist.
    :raises ValueError: If the file is empty.
    """
    if not os.path.exists(PAGE_FILE) or os.stat(PAGE_FILE).st_size == 0:
        raise FileNotFoundError("The file does not exist or is empty.")

    with open(PAGE_FILE, "r") as file:
        return int(file.read().strip())
