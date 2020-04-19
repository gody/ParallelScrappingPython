import time
import random
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver

import threading

csv_writer_lock = threading.Lock()

options = webdriver.ChromeOptions()
options.add_argument("--ignore-certificate-errors")
options.add_argument("--incognito")
options.add_argument("--headless")

options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")


# url = "https://www.imdb.com/title/tt7052634/reviews?ref_=tt_urv"


def get_reviews(url, id):
    driver = webdriver.Chrome(executable_path="chromedriver", options=options)
    driver.get(url)
    while True:
        try:
            driver.find_element_by_id("load-more-trigger").click()
            time.sleep(random.randint(1, 3))
        except Exception as e:
            print(e)
            break

    test = driver.find_elements_by_css_selector("div.text.show-more__control")
    reviews = [[id] + [c.text] for c in test]
    print(id, len(reviews))
    with csv_writer_lock:
        with open("all_reviews.csv", mode="a") as f1:
            review_writer = csv.writer(f1, delimiter=",")
            for r in reviews:
                review_writer.writerow(r)
    return pd.DataFrame(reviews)


def set_up_threads(urls, cores):
    """
    Create a thread pool and download specified urls
    """

    with ThreadPoolExecutor(max_workers=cores) as executor:
        return executor.map(get_reviews, urls["URLS"], urls["movieId"], timeout=60)


if __name__ == "__main__":
    # read and generate urls
    review_count = pd.read_csv("review.csv")

    review_count["URLS"] = [
        "https://www.imdb.com/title/tt" + str(id).zfill(7) + "/reviews?ref_=tt_urv"
        for id in review_count["imdbId"]
    ]
    print(len(review_count))
    reviews100 = review_count[review_count["reviews"] > 95]
    max_movies = 50

    times = [0, 0, 0, 0]
    cores = [1, 3, 5, 7]
    for max_movies in [50, 100, 250]:
        for j in range(1, 5):
            i = 0
            for core in cores:
                random.seed(1234)
                tic = time.perf_counter()
                s
                et_up_threads(reviews100[0:max_movies], core)
                toc = time.perf_counter()
                times[i] = toc - tic
                i = i + 1
            with open("times.csv", mode="a") as f1:
                review_writer = csv.writer(f1, delimiter=",")
                review_writer.writerow(times)
