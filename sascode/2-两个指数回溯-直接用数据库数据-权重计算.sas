/******************* 模块4-2: 娱乐指数从备选池中筛选出成份股 ************/
%LET improve_pct = 0;
/** 回测日期 **/
PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;
DATA adjust_busdate;
	SET adjust_busdate;
	IF "15sep2014"d <= end_date <= "15feb2015"d;
RUN;
/**/
/*DATA sub_test(rename = (end_date = date));*/
/*	SET product.taobao_score;*/
/*	weight = 1;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/

/**　用文汉版本，需要取个股得分　*/
PROC SQL;
	CREATE TABLE sub_test AS
	SELECT A.*, B.tot_score
	FROM fgtest.fg_taobao_index A LEFT JOIN fgtest.fg_taobao_map C
	ON A.effective_date = C.end_date AND A.stock_code = C.stock_code AND C.fund_name = "TAOBAO_ENTERTAIN"
	LEFT JOIN fgtest.fg_taobao_ind_score B
	ON A.effective_date = B.end_date AND C.indus_code = B.indus_code 
	WHERE A.fund_name = "TAOBAO_ENTERTAIN" 
	ORDER BY A.effective_date, A.stock_code;
QUIT;

/*DATA tt;*/
/*	SET fgtest.fg_taobao_index;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT effective_date, count(1) AS nobs*/
/*	FROM fgtest.fg_taobao_index*/
/*	WHERE fund_name = "TAOBAO_ENTERTAIN"*/
/*	GROUP BY effective_date*/
/*	ORDER BY effective_date;*/
/*QUIT;*/


DATA sub_test(rename = (effective_date = date));
	SET sub_test;
	weight = 1;
	effective_date = datepart(effective_date);
	FORMAT effective_date yymmdd10.;
RUN;

PROC SORT DATA = sub_test;
	BY date stock_code;
RUN;



/** 1- 基准部分 **/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.freeshare, C.close
	FROM sub_test A LEFT JOIN fg_wind_freeshare B
	ON A.stock_code = B.stock_code AND A.date = datepart(end_date)
	LEFT JOIN hqinfo C
	ON A.stock_code = C.stock_code AND A.date = C.end_date
	ORDER BY A.date, A.stock_code;
QUIT;

DATA test_stock_pool(drop = freevalue freeshare close);
	SET tmp;
	freevalue = freeshare * close;
	IF not missing(freevalue) THEN weight = freevalue;
	ELSE weight = 0;
RUN;


%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);

DATA base_stock_pool;
	SET test_stock_pool;
RUN;
PROC SORT DATA = base_stock_pool;
	BY end_date;
RUN;


/** 2. 增强部分 **/
%LET mark_table = score_half;
%LET is_equal =0;
%LET fname = tot_score;

/** 创建辅助表：标注得分是否大于位于前50%行业 */
/*DATA factors_value;*/
/*	SET product.taobao_score_i;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/

DATA factors_value;
	SET fgtest.fg_taobao_ind_score;
	end_Date = datepart(end_Date);
	FORMAT end_Date datetime20.;
RUN;
/* 取indus_name信息 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.indus_name
	FROM factors_value A LEFT JOIN fgtest.fg_taobao_indus B
	ON A.indus_code = B.indus_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.end_date, B.indus_name;
QUIT;
DATA factors_value;
	SET tmp;
RUN;



PROC SORT DATA = factors_value OUT = factors_value;
	BY end_date;
RUN;

PROC UNIVARIATE DATA = factors_value NOPRINT;
	BY end_date;
	VAR tot_score;
	OUTPUT OUT = tmp median = tot_score_m;
RUN;
PROC SQL;
	CREATe TABLE score_half AS
	SELECT A.end_date, A.indus_name, A.indus_code, 
		A.tot_score, B.tot_score_m
	FROM factors_value A LEFT JOIN tmp B
	ON A.end_date = B.end_date
	ORDER BY A.end_date, A.indus_name;
QUIT;
DATA score_half(keep = end_date indus_code indus_name tot_score);
	SET score_half;
	ARRAY var_list(1) tot_score;
	ARRAY var_list_m(1) tot_score_m;
	DO i = 1 TO 1;
		IF not missing(var_list(i)) AND var_list(i) > 0 AND var_list(i)>var_list_m(i) THEN var_list(i) = 1;
		ELSE var_list(i) = 0;
	END;
RUN;

/** 取行业信息 */
/*PROC SQL;*/
/*	CREATE TABLE test_stock_pool AS*/
/*	SELECT A.*, B.indus_code, B.indus_name, C.&fname AS &fname._score*/
/*	FROM sub_test A LEFT JOIN product.taobao_mapping B*/
/*	ON A.date = B.end_date AND A.stock_code = B.stock_code*/
/*	LEFT JOIN &mark_table. C*/
/*	ON A.date = C.end_date AND B.indus_code = C.indus_code*/
/*	WHERE B.fund_name = "TAOBAO_ENTERTAIN"*/
/*	ORDER BY A.date, B.indus_code;*/
/*QUIT;*/

PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.*, B.indus_code, C.&fname AS &fname._score, D.indus_name
	FROM sub_test A LEFT JOIN fgtest.fg_taobao_map B
	ON A.date = datepart(B.end_date) AND A.stock_code = B.stock_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	LEFT JOIN &mark_table. C
	ON A.date = C.end_date AND B.indus_code = C.indus_code
	LEFT JOIN fgtest.fg_taobao_indus D
	ON B.indus_code = D.indus_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.date, B.indus_code;
QUIT;



PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.freeshare, C.close
	FROM test_stock_pool A LEFT JOIN fg_wind_freeshare B
	ON A.stock_code = B.stock_code AND A.date = datepart(end_date)
	LEFT JOIN hqinfo C
	ON A.stock_code = C.stock_code AND A.date = C.end_date
	ORDER BY A.date, A.stock_code;
QUIT;

DATA test_stock_pool(drop = freevalue freeshare close);
	SET tmp;
	freevalue = freeshare * close;
	IF not missing(freevalue) THEN weight = freevalue;
	ELSE weight = 0;
RUN;


%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	/** 行业增强 */
PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.indus_wt, C.indus_score, C.indus_equal, D.sum_indus_score, D.sum_indus_equal
		FROM test_stock_pool A LEFT JOIN
		(SELECT end_date, indus_name, sum(weight) AS indus_wt
		FROM test_stock_pool
		GROUP BY end_date, indus_name) B
		ON A.end_date = B.end_date AND A.indus_name = B.indus_name
		LEFT JOIN
		(SELECT end_date, indus_name, 
		mean(&fname._score * &fname.) AS indus_score, /* 得分加权 */
		mean(&fname._score) AS indus_equal /* 等权 */
		FROM test_stock_pool
		GROUP BY end_date, indus_name) C
		ON A.end_date = C.end_date AND A.indus_name = C.indus_name
		LEFT JOIN
		(SELECT end_date, 
		sum(&fname._score * &fname.) AS sum_indus_score, 
		sum(&fname._score) AS sum_indus_equal 
		FROM test_stock_pool
		GROUP BY end_date) D
		ON A.end_date = D.end_date
		ORDER BY A.end_date, A.indus_name;
QUIT;

	/** 增强 */
	DATA test_stock_pool;
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* 等权 */
			IF sum_indus_equal = 0 THEN DO;  /* 没有选中任何行业，就按照基准配 */
				adj_indus_wt = indus_wt;
				multiplier = 1;
				adj_weight = weight * multiplier;
			END;
			ELSE DO;
				adj_indus_wt = indus_equal;
				multiplier = adj_indus_wt / indus_wt;
				adj_weight = weight * multiplier;
			END;
		END;
		ELSE IF &is_equal. = 0 THEN DO;
			IF sum_indus_score = 0 THEN DO;  /* 没有选中任何行业，就按照基准配 */
				adj_indus_wt = indus_wt;
				multiplier = 1;
				adj_weight = weight * multiplier;
			END;
			ELSE DO;
				adj_indus_wt = indus_score; /* 得分加权 */
				multiplier = adj_indus_wt / indus_wt;
				adj_weight = weight * multiplier;
			END;
		END;
		/** 更新权重 */
		weight = adj_weight;
	RUN;
	/** 重新调整下权重，让总权重为1 */
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
DATA im_stock_pool;
	SET test_stock_pool;
RUN;
PROC SORT DATA = im_stock_pool;
	BY end_date;
RUN;


/** 基准+增强 */

PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.stock_code, A.end_date, A.effective_date, A.weight AS base_wt, B.weight AS im_wt
	FROM base_stock_pool A LEFT JOIN im_stock_pool B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA test_stock_pool(drop = im_wt base_wt);
	SET test_stock_pool;
	weight = base_wt * (1-&improve_pct.) + im_wt * &improve_pct.;
	IF weight = 0 THEN delete;
RUN;
PROC SORT DATA = test_stock_pool;
	BY end_date stock_code;
RUN;


/** 避免极端情况，所有股票的im_wt都为0 **/
/** 其实这个步骤也可以不用。因为在增强部分设定了，如果没有选中行业则还按照基准配置。**/
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);

/* 限制个股权重 */
%limit_adjust_stock_only(stock_pool=test_stock_pool, stock_upper=0.1, stock_lower=0, output_stock_pool=test_stock_pool);

DATA entertain_pool;
	SET test_stock_pool;
	LENGTH fund_name $32. ;
	fund_name = "TAOBAO_ENTERTAIN";
RUN;
PROC SORT DATA = entertain_pool;
	BY end_date descending weight;
RUN;
DATA product.taobao_stock_pool;
	SET entertain_pool;
RUN;
