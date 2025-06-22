import pandas as pd
import requests
from lxml import etree
import time
from collections import deque
import os

# 정규 시즌 시작일 딕셔너리
REGULAR_SEASON_START_DATES = {
    2015: "20150328",
    2016: "20160401",
    2017: "20170331",
    2018: "20180324",
    2019: "20190323",
    2020: "20200505",
    2021: "20210403",
    2022: "20220402",
    2023: "20230401",
    2024: "20240323",
    2025: "20250322",
}

# 팀 이름과 네이버 스포츠에서 사용하는 팀 코드를 매핑한 딕셔너리
TEAM_CODES = {
    "LG": "LG", "한화": "HH", "롯데": "LT", "KIA": "HT", "SSG": "SK",
    "삼성": "SS", "KT": "KT", "NC": "NC", "두산": "OB", "키움": "WO",
    
}

# --- 개별 팀 데이터 크롤링 함수 ---
def scrape_team_data(team_code, year, start_date):
    BASE_URL = "https://m.sports.naver.com/team/schedule?category=kbo&teamCode={team_code}&date={date}"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
    }
    session = requests.Session()
    session.headers.update(HEADERS)

    game_results = []
    processed_game_ids = set() 
    crawled_url_dates = set()
    dates_to_crawl = deque([str(year)])

    # 방문할 날짜 큐가 빌 때까지 루프 실행
    while dates_to_crawl:
        current_date_param = dates_to_crawl.popleft()
        if current_date_param in crawled_url_dates: continue
        crawled_url_dates.add(current_date_param)

        url = BASE_URL.format(team_code=team_code, date=current_date_param)
        response = session.get(url)
        tree = etree.HTML(response.content)
        articles = tree.xpath("//article[@id='dataList']")[0].xpath("./article")

        for article in articles:
            link_list = article.xpath("./a")
            if not link_list: continue
            
            href = link_list[0].get('href')
            game_id = href.split('/')[-1] # 고유 게임 ID 추출 (예: '20240421WOKT02024')
            game_date = game_id[:8]       # 날짜 추출 (예: '20240421')

            if not game_date.startswith(str(year)): break  # 연도가 다르게 표기되어 있는 포스트시즌이 나오면 탐색 종료
            if game_date < start_date: continue  # 정규 시즌 시작일 이전의 시범 경기 건너뜀
            if game_id in processed_game_ids: continue  # 이미 처리한 게임 ID는 건너뜀

            result_span_list = article.xpath("./a/div[2]/p[1]/span")   # 경기 결과(승/패/무/취소)가 담긴 span 태그 탐색
            if not result_span_list or not result_span_list[0].get('class'): break  # 결과 span이 없거나, class 속성이 없으면 미래 경기이므로 페이지 처리 중단
            result = result_span_list[0].get('class')
            game_results.append({"date": game_date, "result": result})
            processed_game_ids.add(game_id)

        else: # for 루프가 break 없이 정상적으로 모두 실행되었을 경우
            # 현재 페이지의 마지막 경기가 유효한 연도 데이터이면, 다음 탐색 대상으로 추가
            if articles:
                last_game_date = articles[-1].xpath("./a")[0].get('href').split('/')[-1][:8]
                if last_game_date.startswith(str(year)) and last_game_date not in crawled_url_dates:
                    dates_to_crawl.append(last_game_date)

        time.sleep(0.5)
        
    if not game_results: return pd.DataFrame()

    # --- 수집된 데이터 가공 ---
    df = pd.DataFrame(game_results).sort_values(by="date").reset_index(drop=True)
    df['is_win'] = (df['result'] == 'w').astype(int)
    df['is_lose'] = (df['result'] == 'l').astype(int)
    df_played = df[df['result'] != 'c'].copy() # 취소된 경기는 제외
    df_played['wins'] = df_played['is_win'].cumsum() # cumsum()을 사용하여 누적 승/패 계산
    df_played['losses'] = df_played['is_lose'].cumsum()
    df_played['games'] = range(1, len(df_played) + 1)

    total_for_rate = df_played['wins'] + df_played['losses']
    df_played['win_rate'] = df_played['wins'].divide(total_for_rate).fillna(0)
    df_played['margin'] = df_played['wins'] - df_played['losses']
    
    df_played['team_name'] = team_code
    return df_played[['team_name', 'date', 'games', 'win_rate', 'margin']]

# --- 메인 제어 함수 ---
def generate_kbo_records(year, options='all'):
    """
    특정 연도의 KBO 승률 및 승패마진을 날짜별 또는 경기수별로 크롤링하여 CSV 파일로 저장합니다.
    Args:
        year (int): 크롤링할 연도
        options (str): 수집할 데이터 종류. 'all', 'date', 'game' 중 선택.
                       'date': 날짜별 데이터만 저장
                       'game': 경기수별 데이터만 저장
                       'all': 둘 다 저장 (기본값)
    """
    if options not in ['all', 'date', 'game']:
        print("오류: 'options' 파라미터는 'all', 'date', 'game' 중 하나여야 합니다.")
        return
        
    if year not in REGULAR_SEASON_START_DATES:
        print(f"2015~2025년 사이의 데이터만 지원합니다. {year}년은 지원하지 않습니다.")
        return
        
    start_date = REGULAR_SEASON_START_DATES[year]
    all_teams_data = []

    # 1. 정의된 모든 팀에 대해 데이터 크롤링 실행
    for team_name, team_code in TEAM_CODES.items():
        print(f"--- {year}년 {team_name} 데이터 수집 시작 ---")
        team_df = scrape_team_data(team_code, year, start_date)
        if not team_df.empty:
            all_teams_data.append(team_df)
    
    if not all_teams_data:
        print("수집된 데이터가 없습니다. 프로그램을 종료합니다.")
        return

    print(f"\n--- 데이터 처리 및 파일 저장 시작 (옵션: {options}) ---")
    combined_df = pd.concat(all_teams_data, ignore_index=True)
    combined_df['team_name'] = combined_df['team_name'].map({v: k for k, v in TEAM_CODES.items()})  # 팀 이름을 TEAM_CODES에 정의된 한글 이름으로 변경

    final_stats_indices = combined_df.loc[combined_df.groupby('team_name')['games'].idxmax()]  # 각 팀별로 가장 마지막 경기의 인덱스를 찾음
    ranked_teams = final_stats_indices.sort_values(by='win_rate', ascending=False)  # 최종 승률을 기준으로 내림차순 정렬하여 순위 결정
    ranked_team_names = ranked_teams['team_name'].tolist()

    output_dir = os.path.join(".", "data", str(year))
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. '경기수별' 데이터 처리 및 저장
    if options in ['all', 'game']:
        # index: 경기 수, columns: 팀 이름, values: 승률/승패마진 으로 데이터프레임 재구성
        winrate_by_game = combined_df.pivot_table(index='games', columns='team_name', values='win_rate')
        margin_by_game = combined_df.pivot_table(index='games', columns='team_name', values='margin')
        
        # 경기 수가 다른 팀 때문에 생기는 빈 값(NaN)을 이전 값으로 채움
        winrate_by_game.ffill(inplace=True)
        margin_by_game.ffill(inplace=True)

        # 팀 순위에 따라 열 순서 정렬
        winrate_by_game = winrate_by_game[ranked_team_names]
        margin_by_game = margin_by_game[ranked_team_names]

        winrate_by_game = winrate_by_game.map(lambda x: f"{x:.3f}" if pd.notna(x) else '')   # 승률: 소숫점 3자리 문자열로 포맷팅
        margin_by_game = margin_by_game.fillna(0).astype(int)  # 승패마진: float을 int로 변경 (NaN은 0으로 처리)

        winrate_by_game.to_csv(os.path.join(output_dir, f"winrate_game_{year}.csv"))
        margin_by_game.to_csv(os.path.join(output_dir, f"margin_game_{year}.csv"))
        print(f"'{output_dir}' 폴더에 경기수별 데이터 저장 완료")

    # 3. '날짜별' 데이터 처리 및 저장
    if options in ['all', 'date']:
        # aggfunc='last': 더블헤더 등 하루 여러 경기가 있을 때, 평균이 아닌 마지막 값을 선택
        winrate_by_date = combined_df.pivot_table(index='date', columns='team_name', values='win_rate', aggfunc='last')
        margin_by_date = combined_df.pivot_table(index='date', columns='team_name', values='margin', aggfunc='last')
        
        # 경기가 없던 날의 빈 값(NaN)을 가장 최근의 기록으로 채움 (forward fill)
        winrate_by_date.ffill(inplace=True)
        margin_by_date.ffill(inplace=True)

        winrate_by_date = winrate_by_date[ranked_team_names]
        margin_by_date = margin_by_date[ranked_team_names]
        
        winrate_by_date = winrate_by_date.map(lambda x: f"{x:.3f}" if pd.notna(x) else '')
        margin_by_date = margin_by_date.fillna(0).astype(int)
        
        winrate_by_date.to_csv(os.path.join(output_dir, f"winrate_date_{year}.csv"))
        margin_by_date.to_csv(os.path.join(output_dir, f"margin_date_{year}.csv"))
        print(f"'{output_dir}' 폴더에 날짜별 데이터 저장 완료")

# --- 4. 스크립트 실행 ---
if __name__ == "__main__":
    year = int(input("크롤링할 연도를 입력하세요 (2015~2025): "))
    generate_kbo_records(year, options='all')
