/******************* 模块4-2: 娱乐指数从备选池中筛选出成份股 ************/
%LET improve_pct = 0;
%LET indus_code = 10070;
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
	IF "15jan2014"d <= end_date <= "15feb2015"d;
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
	SELECT A.*, B.tot_score, C.indus_code
	FROM fgtest.fg_taobao_index A LEFT JOIN fgtest.fg_taobao_map C
	ON A.effective_date = C.end_date AND A.stock_code = C.stock_code AND C.fund_name = "TAOBAO_ENTERTAIN"
	LEFT JOIN fgtest.fg_taobao_ind_score B
	ON A.effective_date = B.end_date AND C.indus_code = B.indus_code 
/*	WHERE A.fund_name = "TAOBAO_ENTERTAIN" AND C.indus_code = &indus_code.*/
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

/** 计算行业权重 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, indus_code, sum(weight) AS indus_weight
	FROM base_stock_pool
	WHERE end_date = "30jan2015"d
	GROUP BY end_Date, indus_code
	ORDER BY end_date, indus_weight desc;
QUIT;


DATA entertain_pool;
/*	LENGTH fund_name $32. ;*/
	SET test_stock_pool;
	fund_name = "TAOBAO_ENTERTAIN";
RUN;
PROC SORT DATA = entertain_pool;
	BY end_date descending weight;
RUN;
DATA product.taobao_stock_pool;
	SET entertain_pool;
RUN;

/**** 构建策略 ***/
/** 优先运行： 两个指数标准化流程-依据说明文档-完整版本 **/

/*** Step1: 从调仓记录中提取所需的指数数据 */
%LET fname = TAOBAO_ENTERTAIN;
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT end_date, stock_code, weight
	FROM product.taobao_stock_pool
	WHERE fund_name = "&fname.";
QUIT;



/*** Step2: 回测区间的各种参数 */

/* (1) 回测日期: 2010/1/1 -2014/11/30 */
DATA test_busdate;
	SET busday(keep = date);
	IF "01feb2015"d <= date <= "28feb2015"d;
RUN;

/* (2) 调仓日期: 每个月月末 */
/* 月末数据 */
PROC SQL;
	CREATE TABLE month_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;

DATA adjust_busdate;
	SET month_busdate;
	IF "15jan2014"d <= end_date <= "15feb2015"d;
RUN;


/** Step3: 构建指数 **/

%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool)
%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=&fname._im);
%trading_summary(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, 
	output_stock_trading=&fname._stock_trading, output_daily_trading=&fname._daily_trading);

libname myxls "&output_dir.\&fname._index.xls";
	DATA myxls.index;
		SET &fname._im;
		FORMAT date yymmdd10.;
	RUN;
	DATA myxls.trading;
		SET &fname._daily_trading;
		FORMAT date yymmdd10.;
	RUN;
LIBNAME myxls CLEAR;




