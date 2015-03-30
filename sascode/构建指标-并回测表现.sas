/**** 构建策略 ***/
/** 优先运行： 两个指数标准化流程-依据说明文档-完整版本 **/
%LET product_dir = D:\Research\淘消费\阿里-产品;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\淘消费\sascode\指数构建-标准版\指数构建标准版.sas";
options validvarname=any; /* 支持中文变量名 */


/*** Step1: 从调仓记录中提取所需的指数数据 */
%LET fname = TAOBAO_ENTERTAIN;
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT end_date, stock_code, stock_name, weight
	FROM product.taobao_stock_pool
	WHERE fund_name = "&fname.";
QUIT;

/*** Step2: 回测区间的各种参数 */

/* (1) 回测日期: 2010/1/1 -2014/11/30 */
DATA test_busdate;
	SET busday(keep = date);
	IF "01feb2011"d <= date <= "30jan2015"d;
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
	IF "31jan2011"d <= end_date <= "31dec2014"d;
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




