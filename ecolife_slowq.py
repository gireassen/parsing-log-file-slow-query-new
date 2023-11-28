import csv
import re

with open('slow-queries.2023-11-09.log', 'r', encoding='utf-8') as f:
    log_data = f.read()

pattern = r"(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2}.\d{3})" \
          r" (WARNING \[.*\] org.apache.tomcat.jdbc.pool.interceptor.SlowQueryReport.reportSlowQuery)" \
          r" (Slow Query Report SQL=.*); time=(\d+ ms);"

matches = re.findall(pattern, log_data, re.DOTALL)

with open('parsed_log2.csv', 'w', newline='', encoding='utf-8') as csvfile:
    log_writer = csv.writer(csvfile, delimiter='\t')
    log_writer.writerow(["Дата", "Ошибка", "Запрос", "Длительность"])
    
    for match in matches:
        date = match[0]
        error = match[1]
        query = match[2]
        duration = match[3]
        
        log_writer.writerow([date, error, query, duration])
