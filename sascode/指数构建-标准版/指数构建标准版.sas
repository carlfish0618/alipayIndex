/** 组合构建_绩效分析_1.0 **/

%LET trans_cost = 0.35;  /** 单边 **/

/** 输入: 
(1) 调整日新的股票池 stock_pool: date/stock_code/weight
(2) 所有的调整日期列表 adjust_date_table: date 
(3) 其他全局变量: index_code/stock_sector_mapping(table)/index_component(table)/hqinfo(table)/benchmark_hqinfo(table)/busday(table)
**/



/*** 模块0: 完善主动调仓记录，生成end_date,effective_date **/
/** 全局表: 
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


/*** 模块2: 将weight进行标准化处理 **/
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

/** 模块3: 设定子主题及个股偏离限制 **/
/** 输入: 
(1) stock_pool: end_date / effective_date(可选) / stock_code/ weight /indus_code/indus_name
(2) indus_limit: 单独行业在组合中的最大权重
(3)  stock_limit: 个股在其归属行业中的最大权重
/** 输出:
(1) output_stock_pool: end_date/effective_date(可选)/stock_code/weight(调整后)/indus_code/indus_name  **/

%MACRO limit_adjust(stock_pool, indus_upper, indus_lower,  stock_limit, output_stock_pool);
	/** 限定行业权重 **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT end_date, indus_code, sum(weight) AS indus_weight
		FROM &stock_pool.
		GROUP BY end_date, indus_code;
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
		
		/* Step1：降低触上限行业 */
		/** 超过上限的行业 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
			GROUP BY end_date;
		QUIT;

		/** 低于上限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight > &indus_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_upper.;
				END;
				ELSE IF indus_weight < &indus_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &indus_upper. * &big_nobs.);
					indus_weight = indus_weight + (indus_weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** 超过上限的行业 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的行业 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: 增加，触下限行业 */
		/** 超过下限的行业 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
			GROUP BY end_date;
		QUIT;

		/** 低于上限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight < &indus_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_lower.;
				END;
				ELSE IF indus_weight > &indus_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&indus_lower. * &small_nobs. - &small_wt.);
					indus_weight = indus_weight - (indus_weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** 超过下限的行业 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的行业 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;
	
	/** 把每个行业的权重，匹配到个股。要求个股权重若超过设定权重，则在其归属的行业内部调整 **/
	PROC SQL;
		CREATE TABLE stock_info AS
		SELECT A.*, B.indus_weight, C.indus_weight_raw
		FROM &stock_pool. A LEFT JOIN indus_info B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code
		LEFT JOIN 
		(SELECT end_date, indus_code, sum(weight) AS indus_weight_raw
		FROM &stock_pool.
		GROUP BY end_date, indus_code ) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.indus_code;
	QUIT;
	/* 按照调整后的行业权重，重新分配行业内个股权重 */
	DATA stock_info(drop = indus_weight_raw);
		SET stock_info;
		weight = weight * indus_weight / indus_weight_raw;
	RUN;
	


	/** 要求个股的最大权重不能超过其所处行业权重的一定比例 */
	/** 每天，分行业处理 */

	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		/* 取行业，分别处理 */

		PROC SQL NOPRINT;
			SELECT distinct indus_name, count(distinct indus_name)
				INTO :indus_list separated by ' ',
					: indus_nobs
			FROM stock_info
			WHERE end_date = input("&curdate.", mmddyy10.);
		QUIT;


		%DO indus_index = 1 %TO &indus_nobs.;
			%LET cur_indus = %SCAN(&indus_list, &indus_index., ' ');

			%LET big_nobs = 0;
			/** 超过限制的个股 **/
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
				INTO :end_date,
					 :indus_code,
					 :big_wt,
				 	 :big_nobs,
					 :indus_wt
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
					AND weight > &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

			%LET small_wt = 0;
			%LET small_nobs = 0;
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*)
				INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
					AND weight < &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

		
			%DO %WHILE (%SYSEVALF(&big_nobs. AND &small_nobs.));
				/** 低于限制的个股 **/
				DATA stock_info;
					MODIFY stock_info;
					IF end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight > &stock_limit. THEN DO;
						weight = &stock_limit.;
					END;
					ELSE IF end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight < &stock_limit. THEN DO;
							%LET large_part = %SYSEVALF(&big_wt. - &stock_limit. * &big_nobs.);
							weight = weight + (weight / &small_wt.) * &large_part.;
					END;
				RUN;

				/** 检验是否还有超过限制的行业 */
				%LET big_nobs = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
					INTO :end_date,
						 :indus_code,
					 	:big_wt,
				 	 	:big_nobs,
					 	:indus_wt
					FROM stock_info
					WHERE end_date = &curdate. AND indus_name = "&cur_indus." 
						AND weight >  &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

				%LET small_nobs = 0;
				%LET small_wt = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*)
					INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
					FROM stock_info
					WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight < &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

			%END;
		%END;
	%END;

	DATA &output_stock_pool;
		SET stock_info(drop = indus_weight);
	RUN;
	PROC SQL;
		DROP TABLE stock_info, indus_info;
	QUIT;

%MEND limit_adjust;

/** 模块3-2: 设定个股权重 **/
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



/*** 模块4: 根据调仓数据生成每天的股票池 */
/** 外部表: 
(1) busday: date **/

/** 输入: 
(1) stock_pool: end_date / effective_date(可选) / stock_code/ weight /(其他)
(2) test_period_table: date (对应的是effective_date)
(3) adjust_date_table: end_date   **/

/** 输出:
(1) output_stock_pool: end_date/effective_date(可选)/stock_code/weight(标准化且调整后)/adjust_date/adjust_weight / (其他) **/

%MACRO gen_daily_pool(stock_pool, test_period_table, adjust_date_table, output_stock_pool );
	/* Step1: 确定相邻调整日期 */
	PROC SORT DATA = &adjust_date_table.;
		BY descending end_date;
	RUN;
	DATA tt_adjust;
		SET &adjust_date_table.;
		next_adj_date = lag(end_date);
		FORMAT next_adj_date mmddyy10.;
	RUN;
	
	/* Step2: 确定对应的调整日 */
	/** 有4种情况:
	(1): 回测期的第一天<=最早调整日 --> 只保留最早调整日之后的回测时间。
	(2): 回测期的第一天>最早调整日，但因为频率不同，与最近调整日之间的间隔超过1个交易日 --> 为避免wt计算的问题，把回测期向前延长到刚好距离最近调整日一个交易日
	(3): 回测期的第一天恰好是某个调整日的下一天。  --> 正常
	(4): 回测期的最后一天早于最早调整日  --> 数据错误，这里暂不处理
	**/ 
	/** 取向前距离第一个回测日最近的调整日(不含)，作为回测的开头，为了后面计算weight的准确性 **/
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT date
		FROM &test_period_table.
		WHERE today() > date > (SELECT min(end_date) FROM &adjust_date_table.);  /* 都只有在最早调整日之后 */
		
		SELECT max(end_date) INTO :nearby_adj
		FROM &adjust_date_table.
		WHERE end_date < (SELECT min(date) FROM tmp);

		CREATE TABLE tt_date_list AS
		SELECT A.date, B.end_date AS adjust_date
		FROM
		(
		SELECT date 
		FROM busday 
		WHERE &nearby_adj. <date <= (SELECT max(date) FROM &test_period_table.)
		) A  
		LEFT JOIN tt_adjust B
		ON B.end_date < A.date <= B.next_adj_date
		ORDER BY A.date;
	QUIT;


	/* Step3: 处理最后一个调整日之后的交易日期 */
	PROC SQL NOPRINT;
		SELECT max(end_date) INTO :adjust_ending
		FROM &adjust_date_table.
	QUIT;
	DATA tt_date_list;
		SET tt_date_list;
		IF missing(adjust_date) THEN adjust_date = &adjust_ending.;
	RUN;


	/* Step4: 与股票池相连 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.date, A.adjust_date, B.*
		FROM tt_date_list A LEFT JOIN &stock_pool. B
		ON A.adjust_date = B.end_date 
		ORDER BY A.date;
	QUIT;
	DATA &output_stock_pool.;
		SET tmp(rename = (weight = adjust_weight) drop = end_date);
	RUN;
	PROC SORT DATA = &output_stock_pool.;
		BY date descending adjust_weight;
	RUN;

	PROC SQL;
		DROP TABLE tt_adjust, tt_date_list, tmp;
	QUIT;
%MEND gen_daily_pool;


/** 模块5: 根据收益率确定组合每天个股准确的weight和单日收益（该版本无需每天迭代计算) **/
/** 外部表: hqinfo
(1) end_date/stock_code/pre_close/close/factor (并且只包含A股股票) */

/** 输入: 
(1) daily_stock_pool: date / stock_code/ adjust_weight/ adjust_date / (其他)
(2) adjust_date_table: end_date   **/

/** 输出:
(1) output_stock_pool: date/stock_code/adjust_weight/adjust_date/其他 + 新增 */

/* 新增有关wt的字段:
(a) 开盘: open_wt (前一天收盘调整后的权重)
(b) 收盘: close_wt 
(c) pre_close_wt
(d) after_close_wt
*/
/* 新增收益率字段: daily_ret/accum_ret/pre_accum_ret(从adjust_date开始) **/
/** 新增辅助字段: pre_date/pre_price/price **/


%MACRO cal_stock_wt_ret(daily_stock_pool, adjust_date_table, output_stock_pool);
	/* Step1: 计算单日收益率，从调整日至今的累计收益率等 */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* 提高效率，提取子集*/
	PROC SQL;
		CREATE TABLE tt_hqinfo AS
		SELECT end_date, stock_code, pre_close, close, factor
		FROM hqinfo
		WHERE end_date >= (SELECT min(end_date)-20 FROM &daily_stock_pool.) 
		AND stock_code IN (SELECT stock_code FROM &daily_stock_pool.)
		ORDER BY end_date, stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stock AS
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price, B.pre_close, B.close,
		(D.close*D.factor) AS adjust_price
		FROM &daily_stock_pool. A
		LEFT JOIN tt E
		ON A.date = E.date
		LEFT JOIN tt_hqinfo B
		ON A.date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hqinfo C
		ON E.pre_date = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hqinfo D
		ON A.adjust_date = D.end_date AND A.stock_code = D.stock_code
		ORDER BY A.date, A.stock_code;
	QUIT;

	DATA tt_summary_stock;
		SET tt_summary_stock;
		IF not missing(price) THEN accum_ret = (price/adjust_price - 1)*100;
		ELSE accum_ret = 0;
		IF not missing(pre_price) THEN pre_accum_ret = (pre_price/adjust_price-1)*100;
		ELSE pre_accum_ret = 0;
		IF not missing(pre_price) AND not missing(price) THEN daily_ret = (price/pre_price - 1)*100; 
		ELSE daily_ret = 0;
	RUN;

	/** ！！ 新增：基于close和pre_close计算累计收益 */
	PROC SORT DATA = tt_summary_stock;
		BY stock_code date;
	RUN;
	DATA tt_summary_stock;
		SET tt_summary_stock;
		BY stock_code;
		mark = 1;
		RETAIN r_last_date .;
		RETAIN r_last_accum_ret .;
		RETAIN r_last_stock_code .;
		IF first.stock_code OR pre_date = adjust_date THEN DO;
			r_last_date = pre_date;
			r_last_accum_ret = 0;
			r_last_stock_code = stock_code;
		END;
		IF r_last_date = pre_date AND r_last_stock_code = stock_code THEN DO;
			pre_accum_ret_c = r_last_accum_ret;
			accum_ret_c = ((1+pre_accum_ret_c/100)*close/pre_close-1)*100;
			daily_ret_c = (close/pre_close-1)*100;
		END;
		ELSE mark = 0;
		r_last_date = date;
		r_last_accum_ret = accum_ret_c;
		r_last_stock_code = stock_code;
	RUN;

	
	
	/* Step2: 计算个股权重 */
	/* Step2-1: 收盘权重（未调整前）*/
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, round((A.adjust_weight*(1+A.accum_ret/100))/B.port_accum_ret,0.00001) AS close_wt,
		round((A.adjust_weight*(1+A.accum_ret_c/100))/B.port_accum_ret_c,0.00001) AS close_wt_c
		FROM tt_summary_stock A LEFT JOIN
		(
		SELECT date, sum(adjust_weight*accum_ret/100)+1 AS port_accum_ret,
		sum(adjust_weight*accum_ret_c/100)+1 AS port_accum_ret_c 
		FROM tt_summary_stock 
		GROUP BY date
		) B
		ON A.date = B.date
		ORDER BY A.date, close_wt desc;
	QUIT;


	/* Step2-2: 开盘权重（已调整后）以及前收盘权重 */
	/* 判断该天的前一天是否为调整日或第一天，如果是，则开盘时为adjust_weight，否则为前一天的收盘权重 */
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT A.*, B.end_date AS adj_date_b, C.close_wt AS pre_close_wt, C.close_wt_c AS pre_close_wt_c
		FROM tt_stock_wt A LEFT JOIN &adjust_date_table. B
		ON A.pre_date = B.end_date
		LEFT JOIN tt_stock_wt C
		ON A.pre_date = C.date AND A.stock_code = C.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;

	DATA tmp(drop = adj_date_b);
		SET tmp;
		IF not missing(adj_date_b) THEN DO;
			open_wt = adjust_weight; 
			open_wt_c = adjust_weight;
		END;
		ELSE DO;
			open_wt = pre_close_wt;
			open_wt_c = pre_close_wt_c;
		END;
		IF missing(pre_close_wt) THEN pre_close_wt = 0;  /* 新增的股票 */
		IF missing(pre_close_wt_c) THEN pre_close_wt_c = 0;
	RUN;
	/* Step2-3：收盘后调整了的权重 */
	/* 下一天的开盘权重 */
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, B.open_wt AS after_close_wt, 
		B.open_wt_c AS after_close_wt_c,
		B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET tt_stock_wt;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* 删除的股票。特殊情况: 最后一天认为是清盘 */
		IF missing(after_close_wt_c) THEN after_close_wt_c = 0;
	RUN;
	
	PROC SQL;
		DROP TABLE tt, tt_hqinfo, tt_summary_stock, tt_stock_wt, tmp;
	QUIT;
%MEND cal_stock_wt_ret;
		
	


/** 模块6: 计算组合的收益和alpha等 */

/** 输入: 
(1) daily_stock_pool: date / stock_code/open_wt/daily_ret / (其他)

/** 输出:
(1) output_daily_summary: date/daily_ret/accum_ret/nstock **/


%MACRO cal_portfolio_ret(daily_stock_pool, output_daily_summary);
	/* 按单日进行统计 */
	PROC SQL;
		CREATE TABLE tt_summary_day AS
		SELECT date, sum(open_wt*daily_ret) AS daily_ret, sum(open_wt>0) AS nstock,
		((sum(adjust_weight*accum_ret/100)+1)/(sum(adjust_weight*pre_accum_ret/100)+1)-1)*100 AS daily_ret_p,
		sum(open_wt_c * daily_ret_c) AS daily_ret_c,
		((sum(adjust_weight*accum_ret_c/100)+1)/(sum(adjust_weight*pre_accum_ret_c/100)+1)-1)*100 AS daily_ret_c_p
		FROM &daily_stock_pool.
		GROUP BY date;
	QUIT;

	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+daily_ret_c/100)-1)*100;  /* ！！以全收益指数计算的收益率，为准*/
		index = 1000 * (1+accum_ret/100);
	RUN;
	DATA &output_daily_summary;
		SET tt_summary_day;
	RUN;

	PROC SQL;
		DROP TABLE tt_summary_day;
	QUIT;
%MEND cal_portfolio_ret;


/** 模块7: 行业等权，个股维持其原本在行业内的权重 */
/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/indus_name

/** 输出:
(1) output_stock_pool: end_date/stock_code/weight/indus_code/indus_name */

%MACRO indus_netrual(stock_pool, output_stock_pool);
	PROC SQL;
		CREATE TABLE tt_pool AS 
		SELECT A.*, B.indus_nobs, C.indus_wt
		FROM &stock_pool. A LEFT JOIN
		(SELECT end_date, count(distinct indus_code) AS indus_nobs
		FROM &stock_pool.
		GROUP BY end_date) B 
		ON A.end_date = B.end_date
		LEFT JOIN
		(SELECT end_date, indus_code, sum(weight) AS indus_wt
		FROM &stock_pool.
		GROUP BY end_date, indus_code) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	DATA &output_stock_pool.(drop = indus_wt indus_nobs);
		SET tt_pool;
		weight = (1/indus_nobs)*(weight/indus_wt);
	RUN;
%MEND indus_netrual;

/** 模块8: 生成交易清单 */
/** 生成两张表: 认为交易发生在date的收盘后
(1) 股票交易清单; 包括date/stock_code/initial_wt/traded_wt/final_wt/status/trade_type/trans_cost(暂定为单边0.35%)
(2) 每天交易清单: 包括date/delete_assets/added_assets/sell_wt/buy_wt/buy_cost/sell_cost/turnover(双边和)
**/
%MACRO trading_summary(daily_stock_pool, adjust_date_table, output_stock_trading, output_daily_trading);
	
	/* Step1: 只考虑调仓日和调仓日之后的记录 */
	/** ！！以全收益计算出的权重为准 */
	PROC SQL;
		CREATE TABLE tt_stock_pool AS
		SELECT date, pre_date, stock_code, 
		close_wt_c AS close_wt, 
		open_wt_c AS open_wt,
		pre_close_wt_c AS pre_close_wt, 
		after_close_wt_c AS after_close_wt
		FROM &daily_stock_pool.
		WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
		OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
	QUIT;
	/* Step2: 确定股票变动*/
	DATA tt_stock_pool;
		SET tt_stock_pool;
		/* 当天 */
		IF after_close_wt = 0 THEN status = -1;  /* 表示剔除 */
		ELSE status = 0; /* 保留 */
		initial_wt = close_wt;
		traded_wt = after_close_wt - close_wt;
		final_wt = after_close_wt;
		IF after_close_wt - close_wt > 0 THEN trade_type = 1;
		ELSE IF after_close_wt = close_wt THEN trade_type = 0;
		ELSE trade_type = -1;
		/* 前一天 */
		IF pre_close_wt = 0 THEN status_p = 1;
		ELSE status_p = 0;
		initial_wt_p = pre_close_wt;
		traded_wt_p = open_wt - pre_close_wt;
		final_wt_p = open_wt;
		IF open_wt - pre_close_wt > 0 THEN trade_type_p = 1;
		ELSE IF open_wt = pre_close_wt  THEN trade_type_p = 0;
		ELSE trade_type_p = -1;
	RUN;

	/* Step3：把所有的调仓都认为发生在收盘后 */
	/* 确定测试期内所有的调仓日 */
	PROC SQL;
		CREATE TABLE tt_adjust AS
		SELECT distinct adjust_date AS date
		FROM &daily_stock_pool.;
	QUIT;
		
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.date, B.stock_code,
		B.status, B.trade_type, B.initial_wt, B.traded_wt, B.final_wt
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.date /* 在第一次调仓时, stock_code_b可能为空 */
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tmp2 AS
		SELECT A.date, B.stock_code, B.status_p, B.trade_type_p, 
		B.initial_wt_p, B.traded_wt_p, B.final_wt_p
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.pre_date
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tt_stock_pool AS  /* 取并集，股票如果没有调入调出的，在tmp1和tmp2中是一样的 */
		SELECT *
		FROM tmp1 UNION
		(SELECT date, stock_code, status_p AS status, trade_type_p AS trade_type,
		initial_wt_p AS initial_wt, traded_wt_p AS traded_wt, final_wt_p AS final_wt
		FROM tmp2) 
		ORDER BY date, stock_code;
	QUIT;
	DATA &output_stock_trading.;
		SET tt_stock_pool;
		trans_cost = abs(traded_wt) * 0.0035;
	RUN;

	/* Step4: 统计每天的情况 */
	PROC SQL;
		CREATE TABLE &output_daily_trading. AS
		SELECT date, sum(status=1) AS added_assets, sum(status=-1) AS deleted_assets,
			sum(traded_wt *(traded_wt>0)) AS buy_wt, 
			- sum(traded_wt * (traded_wt<0)) AS sell_wt,
			sum(traded_wt *(traded_wt>0)) * 0.0035 AS buy_cost,
			- sum(traded_wt * (traded_wt<0)) * 0.0035 AS sell_cost,
			sum(traded_wt *(traded_wt>0)) - sum(traded_wt * (traded_wt<0)) AS turnover
		FROM &output_stock_trading.
		GROUP BY date;
	QUIT;
	
	PROC SQL;
		DROP TABLE tt_stock_pool, tmp1, tmp2, tt_adjust;
	QUIT;
%MEND trading_summary;
