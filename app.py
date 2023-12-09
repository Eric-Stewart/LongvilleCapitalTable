from flask import Flask, render_template, send_file
import psycopg2
import pandas as pd
import os

app = Flask(__name__)

DATABASE_URL = 'postgres://AdminAdmin:AdminAdmin1!@stockdb.postgres.database.azure.com:5432/stockDB'

def get_summary_data(option=1):
	try:
		connection = psycopg2.connect(DATABASE_URL, sslmode='require')
		cursor = connection.cursor()

		# Modify the query according to your summary table name and structure
		query = '''SELECT DISTINCT SPLIT_PART(rm.runName, '_', 1) AS runName
						  ,rm.intervalType
						  ,' ' AS action
						  ,1 AS order
						  ,to_char(((MAX(t)) AT TIME ZONE 'UTC') AT TIME ZONE 'EST', 'DD/MM/YYYY  HH:MM am') AS t_max
					FROM runManager rm
					WHERE rm.t >= (SELECT MAX(t)::timestamp::date FROM runManager)
					  AND rm.intervalType = '1d'
					GROUP BY rm.runName, rm.intervalType
					UNION
					SELECT 'Hourly Data Alerts', ' ', ' ', 2, ' '
					UNION
					SELECT SPLIT_PART(runName, '_', 1)
						  ,'1h'
						  ,action
						  ,3
						  ,to_char((t AT TIME ZONE 'UTC') AT TIME ZONE 'EST', 'DD/MM/YYYY  HH:MM am') AS t_max
					FROM alertData
					WHERE t >= (SELECT MAX(t)::timestamp::date FROM runManager)
					UNION
					SELECT 'Hourly Data', ' ', ' ', 3, ' '
					UNION
					SELECT DISTINCT SPLIT_PART(rm.runName, '_', 1)
						  ,rm.intervalType
						  ,' ' AS action
						  ,4 AS order
						  ,to_char(((MAX(t)) AT TIME ZONE 'UTC') AT TIME ZONE 'EST', 'DD/MM/YYYY  HH:MM am') AS t_max
					FROM runManager rm
					WHERE rm.t >= (SELECT MAX(t)::timestamp::date FROM runManager)
					  AND rm.intervalType = '1h'
					  AND rm.runName NOT IN (SELECT DISTINCT runName FROM alertData
											 WHERE t >= (SELECT MAX(t)::timestamp::date FROM alertData))
					GROUP BY rm.runName, rm.intervalType
					ORDER BY 4, 2, 1'''
		query2 = '''SELECT rtt.*
					FROM reportTableTemp rtt
					 JOIN (SELECT PK
							FROM runManager rm
							WHERE rm.t >= (SELECT MAX(t)::timestamp::date FROM runManager)
							  AND rm.intervalType = '1d') rm
						ON rtt.runManagerPK = rm.pk'''
		if option == 1:
			cursor.execute(query)
			summary_data = cursor.fetchall()
		elif option == 2:	
			cursor.execute(query2)
			summary_data = cursor.fetchall()
		elif option == 3:
			cursor.execute(query2)
			summary_data = [desc[0] for desc in cursor.description]

		cursor.close()
		connection.close()


		return summary_data

	except (Exception, psycopg2.Error) as error:
		print("Error fetching data from PostgreSQL:", error)
		return None

@app.route('/')
def index():
    summary_data = get_summary_data()
    return render_template('index.html', summary_data=summary_data)

@app.route('/download')
def download_summary():
	summary_data = get_summary_data(2)
	headers = get_summary_data(3)
	df = pd.DataFrame(summary_data, columns=headers)  # Convert the data to a pandas DataFrame
	excel_file = 'Longville_Daily.xlsx'
	df.to_excel(excel_file, index=False)  # Save the DataFrame to an Excel file
	return send_file(excel_file, as_attachment=True)

if __name__ == '__main__':
	porty = int(os.environ.get("PORT", 5000))
    
	print("port: " + str(porty))
	app.run(host='0.0.0.0', port=porty, debug=True)
