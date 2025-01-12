import os
import requests
import zipfile
import threading

class DataDownloader:
    def __init__(self, logger, max_retries=3):
        self.logger = logger
        self.max_retries = max_retries

    def download_and_extract(self, url, extract_to='.'):
        local_filename = url.split('/')[-1]
        csv_filename = local_filename.replace('.zip', '.csv')
        csv_filepath = os.path.join(extract_to, csv_filename)
        thread_id = threading.get_ident()

        # Jeśli plik CSV już istnieje, pomijamy pobieranie
        if os.path.exists(csv_filepath):
            self.logger.info(
                f"[Thread {thread_id}] File already exists: {csv_filepath} for URL: {url}"
            )
            return

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(
                    f"[Thread {thread_id}] Attempting to download {url}, attempt {attempt}"
                )
                # Pobieramy plik
                with requests.get(url, stream=True, timeout=10) as r:
                    r.raise_for_status()  # rzuci requests.HTTPError przy 4xx/5xx
                    with open(local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                # Rozpakowanie
                with zipfile.ZipFile(local_filename, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)

                # Usuwamy plik .zip po rozpakowaniu
                os.remove(local_filename)

                self.logger.info(
                    f"[Thread {thread_id}] Downloaded and extracted: {url} on attempt {attempt}"
                )
                return  # Udało się – przerywamy pętlę retry

            except requests.HTTPError as e:
                # Sprawdzamy, czy kod błędu to 404
                if e.response.status_code == 404:
                    self.logger.warning(
                        f"[Thread {thread_id}] File not found (404): {url}"
                    )
                    return  # Kończymy, nie ma sensu retry
                else:
                    self.logger.error(
                        f"[Thread {thread_id}] HTTP error {e.response.status_code} on attempt {attempt} for {url}",
                        exc_info=True
                    )
                    if attempt < self.max_retries:
                        self.logger.warning(
                            f"[Thread {thread_id}] Retrying attempt {attempt + 1} of {self.max_retries} for {url}"
                        )
                    else:
                        self.logger.critical(
                            f"[Thread {thread_id}] Exceeded maximum retries for {url}"
                        )

            except requests.Timeout:
                self.logger.error(
                    f"[Thread {thread_id}] Timeout on attempt {attempt} for {url}",
                    exc_info=True
                )
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"[Thread {thread_id}] Retrying attempt {attempt + 1} of {self.max_retries} for {url}"
                    )
                else:
                    self.logger.critical(
                        f"[Thread {thread_id}] Exceeded maximum retries for {url}"
                    )

            except requests.RequestException as e:
                # Inne błędy sieciowe, np. brak połączenia, reset połączenia itp.
                self.logger.error(
                    f"[Thread {thread_id}] Network error on attempt {attempt} for {url}: {e}",
                    exc_info=True
                )
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"[Thread {thread_id}] Retrying attempt {attempt + 1} of {self.max_retries} for {url}"
                    )
                else:
                    self.logger.critical(
                        f"[Thread {thread_id}] Exceeded maximum retries for {url}"
                    )

            except zipfile.BadZipFile as e:
                self.logger.error(
                    f"[Thread {thread_id}] Invalid ZIP file for {url} on attempt {attempt}: {e}",
                    exc_info=True
                )
                # Usuwamy uszkodzony bądź niepełny plik .zip
                if os.path.exists(local_filename):
                    os.remove(local_filename)
                # Zwykle nie ma sensu retry przy uszkodzonym pliku źródłowym
                return

            except Exception as e:
                # Inne nieoczekiwane błędy (np. brak miejsca na dysku, problem z uprawnieniami itp.)
                self.logger.error(
                    f"[Thread {thread_id}] Unexpected error on attempt {attempt} for {url}: {e}",
                    exc_info=True
                )
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"[Thread {thread_id}] Retrying attempt {attempt + 1} of {self.max_retries} for {url}"
                    )
                else:
                    self.logger.critical(
                        f"[Thread {thread_id}] Exceeded maximum retries for {url}"
                    )

        # Jeśli kod doszedł tu bez `return`, oznacza to, że przekroczono liczbę prób.
        # Możesz dodać ewentualną obsługę/logikę końcową.
        self.logger.error(f"[Thread {thread_id}] Giving up on: {url} after {self.max_retries} attempts.")
