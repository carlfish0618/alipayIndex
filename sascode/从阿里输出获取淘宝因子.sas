/**** �Ӱ��������ȡ�Ա�����ֵ ***/

PROC IMPORT OUT = taobao_index
            DATAFILE= "&input_dir.\���������׼����ָ��.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="���������׼����ָ��$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

/** �������� */
DATA entertain_index(keep = month_id fg_level2 taobao_f);
	SET taobao_index;
	IF fg_level1 = 10000;
	IF fg_level2 = 10060 THEN taobao_f = 0.046*per_qty_cun_tb+0.033;
	IF fg_level2 = 10080 THEN taobao_f = 0.036*buyer_cnt_hb+0.021;
	IF fg_level2 = 10040 THEN taobao_f = 0.025*cnt_cun_tb;
	IF fg_level2 = 10030 THEN taobao_f = 0.037* (buyer_cnt_hb-seller_cnt_hb);
	IF fg_level2 = 10090 THEN taobao_f = 0.089*buyer_cnt_tb-0.065*product_cnt_tb+ 0.026;
	IF fg_level2 = 10100 THEN taobao_f = 0.034*buyer_seller_tb+0.014;
	IF fg_level2 = 10110 THEN taobao_f = 0.022*per_qty_cun_hb + 0.016;
	IF fg_level2 = 10010 THEN taobao_f = 0.023*qty_cun_tb+0.013;
	IF not missing(taobao_f);
RUN;
/** �����׼�� **/
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.multiplier 
	FROM entertain_index A LEFT JOIN
	(SELECT month_id, max(abs(max(taobao_f)), abs(min(taobao_f))) AS multiplier
	FROM entertain_index
	GROUP BY month_id) B
	ON A.month_id = B.month_id
	ORDER BY A.month_id;
QUIT;
DATA entertain_index;
	SET tmp;
	taobao_f = max(taobao_f/multiplier,0);
RUN;

/** ӳ�䵽��ҵ���� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.month_id, A.taobao_f, A.fg_level2, B.indus_name
	FROM entertain_index A LEFT JOIN product.taobao_indus_code B
	ON A.fg_level2 = B.indus_code 
	WHERE B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.month_id;
QUIT;

/** ƽ����ҵ */
PROC TRANSPOSE DATA = tmp OUT = entertain_index(drop = _NAME_);
	VAR taobao_f;
	BY month_id;
	ID indus_name;
RUN;
/** ��ȱʧ����ҵ���� */
DATA entertain_index;
	SET entertain_index;
/*	���� = 0;*/
/*	�������� = 0;*/
	��Ʊ = 0;
	��Ʊ = 0;
	���� = 0;
	������Ƶ = 0;
RUN;


/** ��month_idƥ�䵽������ĩ������ */
DATA entertain_index;
	SET entertain_index;
	month = month_id - floor(month_id/100)*100;
	year = floor(month_id/100);
	month_id2 = mdy(month, 1, year);
	FORMAT month_id2 yymmdd10.;
RUN;

PROC SQL;
	CREATE TABLE month_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.end_date
	FROM entertain_index A LEFT JOIN month_busdate B
	ON A.month_id2 <= B.end_date
	GROUP BY A.month_id2
	HAVING B.end_date = min(B.end_date)
	ORDER BY A.month_id2;
QUIT;
DATA entertain_t(keep = ���� -- ������Ƶ end_date);
	SET tmp;
RUN;
PROC TRANSPOSE DATA = entertain_t OUT = entertain_t(rename=(_NAME_ = fg_level2_name col1 = taobao));
	VAR ���� -- ������Ƶ;
	BY end_date;
RUN;


/**** ���ĺ������ȡ��������ֵ ***/

PROC IMPORT OUT = fg_index
            DATAFILE= "&input_dir.\��������_new2_20150325.xls" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT end_date, fg_score AS tot2, B.indus_name AS fg_level2_name
	FROM fg_index A LEFT JOIN product.taobao_indus_code B
	ON A.indus_code = B.indus_code
	ORDER BY A.end_date;
QUIT;
DATA entertain_f;
	SET tmp;
RUN;
