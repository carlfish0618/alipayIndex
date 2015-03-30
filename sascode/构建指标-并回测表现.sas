/**** �������� ***/
/** �������У� ����ָ����׼������-����˵���ĵ�-�����汾 **/
%LET product_dir = D:\Research\������\����-��Ʒ;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\������\sascode\ָ������-��׼��\ָ��������׼��.sas";
options validvarname=any; /* ֧�����ı����� */


/*** Step1: �ӵ��ּ�¼����ȡ�����ָ������ */
%LET fname = TAOBAO_ENTERTAIN;
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT end_date, stock_code, stock_name, weight
	FROM product.taobao_stock_pool
	WHERE fund_name = "&fname.";
QUIT;

/*** Step2: �ز�����ĸ��ֲ��� */

/* (1) �ز�����: 2010/1/1 -2014/11/30 */
DATA test_busdate;
	SET busday(keep = date);
	IF "01feb2011"d <= date <= "30jan2015"d;
RUN;

/* (2) ��������: ÿ������ĩ */
/* ��ĩ���� */
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


/** Step3: ����ָ�� **/

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




