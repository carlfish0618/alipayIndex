/** 提供给文汉的参考代码。适合于两个指数的搭建 **/
/** 省略了因子生成过程 **/

/*** 说明：该代码包括的输入和输出说明。
(1) 输入: (SAS文件）
(a) taobao_indus_code: 行业代码和简称匹配
(b) taobao_mapping: 个股的行业信息
(c) taobao_score: 个股的因子得分
(d) taobao_score_i: 行业因子得分

(2) 输出: 
(a) med_pool: 医药指数
(b) entertain_pool: 娱乐生活指数
 
(3) 比对结果: 运用我的程序跑出的两个指数的结果，放在taobao_stock_pool中（SAS文件）
***/

%LET product_dir = D:\Research\淘消费\阿里-产品;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\淘消费\sascode\指数构建-标准版\指数构建标准版.sas"; /****** 需要用到的函数 ***/ 
options validvarname=any; /* 支持中文变量名 */

/*********************** 准备基础数据表到本地 *******************/
%LET env_start_date = 31dec2008;

/*** (1) 生成日期列表 */
PROC SQL;
	CREATE TABLE busday AS
	SELECT datepart(end_date) AS date FORMAT mmddyy10. LABEL "date"
	FROM
	(
	SELECT distinct(end_date)
	FROM hq.hqinfo
	WHERE end_date >= dhms("&env_start_date."d, 0,0,0)
	)
	ORDER BY date;
QUIT;


/* (2) 行情表 */
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_date) AS end_date FORMAT yymmdd10. LABEL "end_date", stock_code, close, factor, pre_close
	FROM hq.hqinfo
	WHERE type = 'A' AND end_date >= dhms("&env_start_date."d, 0,0,0)
	ORDER BY end_date, stock_code;
QUIT;

/** (3) 自由流通市值表 */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT stock_code, end_date, freeshare
	FROM tinysoft.fg_wind_freeshare;
QUIT;



/******************* 模块1: 医药指数从备选池中筛选出成份股 ************/
%LET threshold = 50;
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
	IF "15dec2008"d <= end_date <= "31dec2014"d;
RUN;

DATA sub_test(rename = (end_date = date));
	SET product.taobao_score;
	weight = 1;
	IF fund_name = "TAOBAO_MEDICINE";
RUN;
PROC SORT DATA = sub_test;
	BY date stock_code;
RUN;

/** threshold: 设置排名的先后 **/
PROC SORT DATA = sub_test;
	BY date descending tot_score descending fg_score;
RUN;

/** 创建辅助表：标注排名*/
DATA score_rank(keep = date stock_code tot_score);
	SET sub_test;
	BY date;
	RETAIN rank_order 0;
	IF first.date THEN rank_order = 0;
	rank_order + 1;
	IF rank_order <= &threshold. AND tot_score > 0 THEN tot_score= 1;
	ELSE tot_score  = 0;
RUN;


%MACRO single_factor_improve_neat(fname, mark_table, output_table, is_equal);
	/* 确定成分股分组 */
	/** 这里：test_stock_pool作为调仓组合，从下一个交易日生效。所以date和因子的end_date恰好对应 */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT A.*, B.&fname AS &fname._score
		FROM sub_test A LEFT JOIN &mark_table. B
		ON A.date = B.date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.stock_code;
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
		ELSE weight = 0;  /* 一般情况下不会出现这个状况 */
	RUN;

	%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	/** 行业增强 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, D.sum_score, D.sum_equal
		FROM test_stock_pool A LEFT JOIN
		(SELECT end_date, 
		sum(&fname._score * &fname.) AS sum_score, 
		sum(&fname._score) AS sum_equal 
		FROM test_stock_pool
		GROUP BY end_date) D
		ON A.end_date = D.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	/** 增强 */
	DATA test_stock_pool(drop = adj_weight);
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* 等权 */
			IF sum_equal = 0 THEN DO;  /* 没有选中任何股票，就按照基准配(基准是流通市值加权) */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) THEN adj_weight = &fname._score; 	/* 取0或者1 */  
				ELSE adj_weight = 0;
			END;
		END;
		ELSE IF &is_equal. = 0 THEN DO;  /** 得分加权 */
			IF sum_score = 0 THEN DO;  /* 没有选中任何股票，就按照基准配 */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) AND not missing(&fname.) THEN adj_weight = abs(&fname._score * &fname.); /* 绝对值，允许取负得分*/
				ELSE adj_weight = 0;  /** 缺失得分，得分加权方式中，只能令权重为0 */
			END;
		END;	
		/** 更新权重 */
		weight = adj_weight;
		IF weight = 0 THEN delete; /** weight = 0的股票予以剔除*/
	RUN;
	/** 重新调整下权重，让总权重为1 */
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	DATA &output_table.;
		SET test_stock_pool(keep = end_date effective_date stock_code stock_name weight);
		into_time = dhms("31dec2014"d, 0,0,0);
		FORMAT into_time datetime20.;
	RUN;
%MEND single_factor_improve_neat;

%single_factor_improve_neat(fname=tot_score, mark_table=score_rank, output_table=med_pool, is_equal=0);

DATA med_pool;
	SET med_pool;
	LENGTH fund_name $32. ;
	fund_name = "TAOBAO_MEDICINE";
RUN;

/******************* 模块2: 娱乐指数从备选池中筛选出成份股 ************/
%LET improve_pct = 0.5;
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
	IF "15jan2011"d <= end_date <= "30sep2014"d;
RUN;

DATA sub_test(rename = (end_date = date));
	SET product.taobao_score;
	weight = 1;
	IF fund_name = "TAOBAO_ENTERTAIN";
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
DATA factors_value;
	SET product.taobao_score_i;
	IF fund_name = "TAOBAO_ENTERTAIN";
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
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.*, B.indus_code, B.indus_name, C.&fname AS &fname._score
	FROM sub_test A LEFT JOIN product.taobao_mapping B
	ON A.date = B.end_date AND A.stock_code = B.stock_code
	LEFT JOIN &mark_table. C
	ON A.date = C.end_date AND B.indus_code = C.indus_code
	WHERE B.fund_name = "TAOBAO_ENTERTAIN"
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
	SELECT A.stock_code, A.stock_name, A.end_date, A.effective_date, A.weight AS base_wt, B.weight AS im_wt
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

/* 限制个股权重 */
%limit_adjust_stock_only(stock_pool=test_stock_pool, stock_upper=0.1, stock_lower=0, output_stock_pool=test_stock_pool);

DATA entertain_pool;
	SET test_stock_pool;
	LENGTH fund_name $32. ;
	fund_name = "TAOBAO_ENTERTAIN";
	into_time = dhms("31dec2014"d, 0,0,0);
	FORMAT into_time datetime20.;
RUN;
PROC SORT DATA = entertain_pool;
	BY end_date descending weight;
RUN;

