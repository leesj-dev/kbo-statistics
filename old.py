import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# 셀레니움 설정
driver = webdriver.Chrome()
driver.get("https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx")

# 데이터 저장할 딕셔너리 초기화
win_rate = {}
margin = {}

while True:
    # 날짜 가져오기
    date_text = driver.find_element(By.ID, "cphContents_cphContents_cphContents_lblSearchDateTitle").text.strip()

    # 날짜가 2025로 시작하는지 확인
    if not date_text.startswith("2025"):
        break

    # 데이터 저장 딕셔너리 초기화
    win_rate_daily = {}
    margin_daily = {}

    # 10개 팀의 데이터 수집
    for i in range(1, 11):
        table_path = f"//*[@id='cphContents_cphContents_cphContents_udpRecord']/table/tbody/tr[{i}]"

        team_name = driver.find_element(By.XPATH, table_path + "/td[2]").text.strip()
        games = int(driver.find_element(By.XPATH, table_path + "/td[3]").text.strip())
        wins = int(driver.find_element(By.XPATH, table_path + "/td[4]").text.strip())
        losses = int(driver.find_element(By.XPATH, table_path + "/td[5]").text.strip())

        win_rate_daily[team_name] = wins / (wins + losses)
        margin_daily[team_name] = wins - losses

    # 날짜별 데이터 추가
    win_rate[date_text[5:10]] = win_rate_daily
    margin[date_text[5:10]] = margin_daily

    # 이전 날짜로 이동
    driver.find_element(By.ID, "cphContents_cphContents_cphContents_btnPreDate").click()
    time.sleep(2)  # 페이지가 로드되는 시간을 기다림

# 브라우저 종료
driver.quit()

# pandas 데이터프레임으로 변환
df1 = pd.DataFrame.from_dict(win_rate, orient="index")[::-1]  # 역순으로 변경
df2 = pd.DataFrame.from_dict(margin, orient="index")[::-1]  # 역순으로 변경

# csv로 저장
df1.to_csv("./baseball_winrate.csv")
df2.to_csv("./baseball_margin.csv")
