/*** 函数1: 完善主动调仓记录，生成end_date,effective_date **/
/** 全局表: （需提前生成）
(1) busday **/
/** 输入: 
(1) stock_pool: date / stock_code/ weight
(2) adjust_date_table: date (如果stock_pool的日期超出adjust_table的范围，将无效。 如果adjust_table中有stock_pool没有的日期，则认为当天股票池中没有股票)
(3) move_date_forward: 是否需要将date自动往前调整一个交易日，作为end_date  **/
/** 输出:
(1) output_stock_pool: end_date/effective_date/stock_code/weight  **/

/** 特殊说明: 正常情况下，都认为如果股票池信号是在前一天收盘后至12:00生成的，即date是昨天的日期，则设定end_date = date, effective_date为end_date下一个交易日 
  		  特殊情况下，如果股票池信号是在今天0:00-开盘前生成的，即date是今天的日期，则在生成调仓记录的时候，应将date自动往前调整一个交易日。
		  特殊情况的处理主要是为了统一 **/
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
		IF &move_date_forward. = 1 THEN DO;  /* 将end_date设定为date前一天 */
			end_date = pre_date;
			effective_date = date;
		END;
		ELSE DO;
			end_date = date;
			effective_date = next_date;
		END;
		IF missing(effective_date) THEN effective_date = end_date + 1; /** 若是最新的一天，则以明天作为effective_date */
		FORMAT effective_date end_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT *
		FROM tmp2
		WHERE end_date IN
	   (SELECT end_date FROM &adjust_date_table.)  /* 只取调整日 */
		ORDER BY end_date;
	QUIT;
	PROC SQL;
		DROP TABLE tt, tmp2;
	QUIT;
%MEND gen_adjust_pool;


/*** 函数2: 将weight进行标准化处理 **/
/** 输入: 
(1) stock_pool: end_date / effective_date(可选) / stock_code/ weight
/** 输出:
(1) output_stock_pool: end_date/effective_date(可选)/stock_code/weight(调整后)  **/

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

/** 模块3: 设定个股权重上下限 **/
/** 输入: 
(1) stock_pool: end_date / effective_date(可选) / stock_code/ weight 
(2)  stock_limit: 个股权重
/** 输出:
(1) output_stock_pool: end_date/effective_date(可选)/stock_code/weight(调整后)/indus_code/indus_name  **/

%MACRO limit_adjust_stock_only(stock_pool, stock_upper, stock_lower, output_stock_pool);
	/** 限定个股权重 **/
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
	
	/** 每天处理 */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1：降低触上限个股 */
		/** 超过上限的个股 **/
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

		/** 低于上限的个股 **/ 
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

			/** 超过上限的个股 **/
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

			/** 低于下限的个股 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: 增加，触下限个股 */
		/** 超过下限的个股 **/
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

		/** 低于上限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* 低于限制的 */
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

			/** 超过下限的个股 **/
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

			/** 低于下限的行业 **/ 
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
