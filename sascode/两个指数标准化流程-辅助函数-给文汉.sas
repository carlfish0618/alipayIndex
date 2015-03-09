/*** ����1: �����������ּ�¼������end_date,effective_date **/
/** ȫ�ֱ�: ������ǰ���ɣ�
(1) busday **/
/** ����: 
(1) stock_pool: date / stock_code/ weight
(2) adjust_date_table: date (���stock_pool�����ڳ���adjust_table�ķ�Χ������Ч�� ���adjust_table����stock_poolû�е����ڣ�����Ϊ�����Ʊ����û�й�Ʊ)
(3) move_date_forward: �Ƿ���Ҫ��date�Զ���ǰ����һ�������գ���Ϊend_date  **/
/** ���:
(1) output_stock_pool: end_date/effective_date/stock_code/weight  **/

/** ����˵��: ��������£�����Ϊ�����Ʊ���ź�����ǰһ�����̺���12:00���ɵģ���date����������ڣ����趨end_date = date, effective_dateΪend_date��һ�������� 
  		  ��������£������Ʊ���ź����ڽ���0:00-����ǰ���ɵģ���date�ǽ�������ڣ��������ɵ��ּ�¼��ʱ��Ӧ��date�Զ���ǰ����һ�������ա�
		  ��������Ĵ�����Ҫ��Ϊ��ͳһ **/
%MACRO gen_adjust_pool(stock_pool, adjust_date_table, move_date_forward, output_stock_pool);
	DATA tt;
		SET busday;
	RUN;
	PROC SORT DATA = tt;
		BY date;
	RUN;
	DATA tt;
		SET tt;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.pre_date, C.date AS next_date
		FROM &stock_pool. A LEFT JOIN tt B
		ON A.date = B.date
		LEFT JOIN tt C
		ON A.date = C.pre_date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tmp2(drop = pre_date next_date date);
		SET tmp2;
		IF &move_date_forward. = 1 THEN DO;  /* ��end_date�趨Ϊdateǰһ�� */
			end_date = pre_date;
			effective_date = date;
		END;
		ELSE DO;
			end_date = date;
			effective_date = next_date;
		END;
		IF missing(effective_date) THEN effective_date = end_date + 1; /** �������µ�һ�죬����������Ϊeffective_date */
		FORMAT effective_date end_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT *
		FROM tmp2
		WHERE end_date IN
	   (SELECT end_date FROM &adjust_date_table.)  /* ֻȡ������ */
		ORDER BY end_date;
	QUIT;
	PROC SQL;
		DROP TABLE tt, tmp2;
	QUIT;
%MEND gen_adjust_pool;


/*** ����2: ��weight���б�׼������ **/
/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight
/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(������)  **/

%MACRO neutralize_weight(stock_pool, output_stock_pool);
	PROC SQL NOPRINT;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_weight
		FROM &stock_pool. A LEFT JOIN 
		(
		SELECT end_date, sum(weight) AS t_weight
		FROM &stock_pool.
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date;
	QUIT;

	DATA &output_stock_pool.(drop = t_weight);
		SET tmp;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.00001);  
		ELSE weight = 0;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND neutralize_weight;

/** ģ��3: �趨����Ȩ�������� **/
/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight 
(2)  stock_limit: ����Ȩ��
/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(������)/indus_code/indus_name  **/

%MACRO limit_adjust_stock_only(stock_pool, stock_upper, stock_lower, output_stock_pool);
	/** �޶�����Ȩ�� **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT *
		FROM &stock_pool.;
	QUIT;

	PROC SQL NOPRINT;
		SELECT distinct end_date, count(distinct end_date)
		INTO :date_list separated by ' ',
			 :date_nobs
		FROM indus_info;
	QUIT;
	
	/** ÿ�촦�� */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1�����ʹ����޸��� */
		/** �������޵ĸ��� **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
			GROUP BY end_date;
		QUIT;

		/** �������޵ĸ��� **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight > &stock_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_upper.;
				END;
				ELSE IF weight < &stock_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &stock_upper. * &big_nobs.);
					weight = weight + (weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** �������޵ĸ��� **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
				GROUP BY end_date;
			QUIT;

			/** �������޵ĸ��� **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: ���ӣ������޸��� */
		/** �������޵ĸ��� **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
			GROUP BY end_date;
		QUIT;

		/** �������޵���ҵ **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight < &stock_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_lower.;
				END;
				ELSE IF weight > &stock_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&stock_lower. * &small_nobs. - &small_wt.);
					weight = weight - (weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** �������޵ĸ��� **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
				GROUP BY end_date;
			QUIT;

			/** �������޵���ҵ **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;

	DATA &output_stock_pool;
		SET indus_info;
	RUN;
	PROC SQL;
		DROP TABLE indus_info;
	QUIT;

%MEND limit_adjust_stock_only;
