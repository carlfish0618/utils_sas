/** ��Ϲ���_��Ч����_1.0 **/

/** ����: 
(1) ������(����)ʱ�µĹ�Ʊ�� stock_pool: date/stock_code/weight/is_bm(=0)
(2) ���еĵ��������б� adjust_date_table: date (���stock_pool�����ڳ���adjust_table�ķ�Χ������Ч�����adjust_table����stock_poolû�е����ڣ�����Ϊ�����Ʊ����û�й�Ʊ)
(3) ����ȫ�ֱ���: index_code/stock_sector_mapping(table)/index_component(table)/hqinfo(table)/benchmark_hqinfo(table)/busday(table)
**/

/*** ģ��0: �����������ּ�¼������end_date,effective_date **/
/* ��������£�����Ϊ�����Ʊ���ź�����ǰһ�����̺���12:00���ɵģ���date����������ڣ����趨end_date = date, effective_dateΪend_date��һ�������� */
/* ��������£������Ʊ���ź����ڽ���0:00-����ǰ���ɵģ���date�ǽ�������ڣ��������ɵ��ּ�¼��ʱ��Ӧ��date�Զ���ǰ����һ�������ա�
/* ��������Ĵ�����Ҫ��Ϊ��ͳһ */

%LET trans_cost = 0.35;  /** ���� **/
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
		IF missing(effective_date) THEN effective_date = end_date + 1;
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

			
			


/** ģ��1: ��Ե��������б����ݲ�λҪ������ָ�� **/
%MACRO fill_in_index(stock_pool,adjust_date_table, all_max_weight, ind_max_weight, output_stock_pool);
	/* Step1: �������Ȩ�� */
	PROC SQL NOPRINT;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_weight
		FROM &stock_pool. A LEFT JOIN 
		(
		SELECT end_date, sum(weight) AS t_weight
		FROM &stock_pool.
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date
		WHERE is_bm = 0  /* ֻȡ���ɣ���������¸������ɲ��� */
		AND A.end_date IN 
		(SELECT end_date FROM &adjust_date_table.)  /* ֻȡ������ */
		ORDER BY A.end_date;
	QUIT;

	DATA tt_ind_pool(drop = t_weight);
		SET tmp;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.00001);   /* ������Ѿ�������ҵ���Ե�����ģ�����weight���ᷢ���仯 */
		ELSE weight = 0;
		IF weight > &ind_max_weight. THEN weight = &ind_max_weight;  /* �и������� */
	RUN;
	
	/* Step2: ���������Ȩ�غ����¼�����ϼ���Ȩ�ء�������е����գ���Ҫʱ��������Ӧ�Ļ�׼ָ�� */
	PROC SQL;
		CREATE TABLE tt_info AS
		SELECT A.end_date, B.t_weight
		FROM &adjust_date_table. A LEFT JOIN 
		(SELECT end_date, sum(weight) AS t_weight
		FROM tt_ind_pool
		GROUP BY end_date
		) B
		ON A.end_date = B.end_date
		ORDER BY end_date;
	QUIT;

	DATA tt_info;
		SET tt_info;
		IF missing(t_weight) OR round(t_weight,0.01) = 0 THEN DO;
			add_bm_weight = 1;  /* ��λȫΪָ�� */
			multiplier = 0;
		END;
		ELSE IF abs(t_weight-&all_max_weight.)>=0.01 AND round(t_weight-&all_max_weight.,0.01)>=0 THEN DO;  /* �����趨��Ȩ�� */
			add_bm_weight = 1-&all_max_weight.;
			multiplier = round(&all_max_weight./t_weight,0.001);  /* ��λ�ȱ������� */
		END;
		ELSE DO;
			add_bm_weight = 1-t_weight;
			multiplier = 1;
		END;
		/* �������� */
		IF abs(add_bm_weight)<=0.01 THEN DO;
			add_bm_weight = 0;
			multiplier = 1;
		END;
	RUN;

	/* Step3: ���µ�������Ȩ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.multiplier, B.t_weight
		FROM tt_ind_pool A LEFT JOIN tt_info B
		ON A.end_date = B.end_date;
	QUIT;

	DATA tt_ind_pool;
		SET tmp;
		weight = weight * multiplier;
	RUN;

	/* Step4: ����ָ���Ĺ�Ʊ�أ�������: is_bm��stock_code*/
	DATA tt_info;
		SET tt_info(keep = end_date add_bm_weight rename = (add_bm_weight = weight));
		stock_code = "&index_code.";
		is_bm = 1; 
		IF weight > 0;
	RUN;

	DATA &output_stock_pool.;
		SET tt_ind_pool(keep = end_date stock_code is_bm weight) tt_info;
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date is_bm stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp,tt_info, tt_ind_pool;
	QUIT;

%MEND fill_in_index;


/** ģ��2: ��ҵ���Բ��� **/

%MACRO adjust_to_sector_neutral(stock_pool, adjust_date_table, max_within_indus, output_stock_pool);
	/* Step2-1:��������գ���׼����ҵȨ�� */
	PROC SQL;
		CREATE TABLE tt_component AS
		SELECT A.end_date, A.stock_code, A.weight, B.o_code, B.o_name
		FROM index_component A  
		LEFT JOIN stock_sector_mapping B
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date 
		WHERE A.index_code = "&index_code." AND A.end_date IN 
		(SELECT end_date FROM &adjust_date_table.)
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_index_info AS
		SELECT end_date, o_code, o_name, sum(weight)/100 AS sector_weight
		FROM tt_component
		GROUP BY end_date, o_code, o_name;
	QUIT;

	/* Step1: �������Ȩ�� */
	PROC SQL NOPRINT;
		CREATE TABLE tt_stock_pool AS
		SELECT A.*, B.o_code, C.sector_weight    /* ȡ��ҵ�ڻ�׼�е�Ȩ�أ�Ū����1Ϊ��λ*/
		FROM &stock_pool A LEFT JOIN stock_sector_mapping B
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date
		LEFT JOIN tt_index_info C
		ON B.end_date = C.end_date AND B.o_code = C.o_code
		WHERE is_bm = 0  /* ֻȡ���ɣ���������¸������ɲ��� */
		AND A.end_date IN 
		(SELECT end_date FROM &adjust_date_table.)  /* ֻȡ������ */
			ORDER BY A.end_date, A.stock_code;
	QUIT;
	
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT A.*, B.t_indus_weight
		FROM tt_stock_pool A LEFT JOIN 
		(
		SELECT end_date, o_code, sum(weight) AS t_indus_weight
		FROM tt_stock_pool
		GROUP BY end_date, o_code
		)B
		ON A.end_date = B.end_date AND A.o_code = B.o_code
		ORDER BY end_date, stock_code;
	QUIT;

	DATA tt_ind_weight(drop = t_indus_weight);
		SET tmp;
		IF missing(sector_weight) OR round(sector_weight,0.0001) = 0 THEN weight = 0; /* ������ջ�׼��û�и���ҵ�����趨����ҵ�����и���Ȩ��Ϊ0 */
		ELSE IF t_indus_weight ~= 0 THEN weight = round(sector_weight*round(weight/t_indus_weight,0.0001),0.00001);
		ELSE weight = 0;
		IF weight > &max_within_indus. * sector_weight THEN weight = &max_within_indus. * sector_weight;  /* ��ҵ���и������� */
	RUN;
 

	/* Step2: ���������Ȩ�غ����¼�����ҵ����Ȩ�ء�������е����գ���Ҫʱ��������Ӧ����ҵָ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.t_indus_weight
		FROM  tt_index_info A LEFT JOIN
		(
		SELECT end_date, o_code, sum(weight) AS t_indus_weight
		FROM tt_ind_weight
		GROUP BY end_date, o_code
		) B
		ON A.end_date = B.end_date AND A.o_code = B.o_code
		WHERE NOT missing(sector_weight)  /* ��ҵȨ�ؿ���ȱʧ */
		ORDER BY A.end_date, A.o_code;
	QUIT;
 
	DATA tt_indus_weight(keep = end_date o_code add_indus is_bm rename = (o_code = stock_code add_indus = weight));
		SET tmp;
		IF missing(t_indus_weight) OR round(t_indus_weight, 0.0001) = 0 THEN DO;
			add_indus = sector_weight;
		END;
		ELSE IF abs(sector_weight-t_indus_weight)> 0.001 AND round(sector_weight-t_indus_weight,0.001)>0 THEN DO;
			add_indus = round(sector_weight - t_indus_weight,0.0001);
		END;
		ELSE DO;
			add_indus = 0;
		END;
		is_bm = 2;
		/* ������ */
		IF abs(add_indus) <= 0.001 THEN add_indus = 0;
	RUN;
	
	/* Step3: ����ָ���Ĺ�Ʊ�أ�������: is_bm��stock_code*/
	DATA &output_stock_pool.;
		SET tt_ind_weight(keep = end_date stock_code weight is_bm) tt_indus_weight(keep = end_date stock_code weight is_bm);
		IF weight > 0;
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date is_bm stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp, tt_component, tt_indus_weight, tt_ind_weight, tt_index_info, tt_stock_pool;
	QUIT;
%MEND adjust_to_sector_neutral;


/** ģ��3: ��ָ���滻Ϊ��Ӧ�ɷֹ� */
/** �����ṩ�Ĺ�Ʊ�أ��Ѿ��������е����յĳ����� */

%MACRO fill_stock(stock_pool, adjust_date_table, output_stock_pool);
	
	/** Step1: �ɷֹ��п۳��Ѿ�������г��ֵĹ�Ʊ�������ڵ����� */
	PROC SQL;
		CREATE TABLE tt_sub_index AS
		SELECT A.end_date, A.stock_code, A.weight, B.stock_code AS stock_code_b
		FROM index_component A
		LEFT JOIN &stock_pool. B
		ON A.end_date = B.end_date AND A.stock_code = B.stock_code 
		WHERE A.index_code = "&index_code." AND A.end_date IN 
		(SELECT end_date FROM &adjust_date_table.)
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	DATA tt_sub_index;
		SET tt_sub_index;
		IF stock_code = stock_code_b THEN delete;
	RUN;
	
	/* Step2: ����ʣ��ɷֹ�����ҵ�е�Ȩ�� */

	/* ʣ��ɷֹ���ҵ��Ϣ */
	PROC SQL;
		CREATE TABLE tt_component AS
		SELECT A.*, B.o_code, B.o_name
		FROM tt_sub_index A  
		LEFT JOIN stock_sector_mapping B
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date 
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	

	/* ��������ҵ�е�Ȩ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.sector_weight, A.weight/B.sector_weight AS wt_in_indus 
		FROM tt_component A LEFT JOIN 
		(
		SELECT end_date, o_code, o_name, sum(weight) AS sector_weight
		FROM tt_component
		GROUP BY end_date, o_code, o_name
		)B
		ON A.end_date = B.end_date AND A.o_code = B.o_code
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	
	/*  ȥ����Щû����ҵ��Ϣ�Ĺ�Ʊ */
	DATA tt_component;
		SET tmp;
		IF not missing(o_code) AND o_code ~= "";
	RUN;
	
	/* Step3: ��Ի�׼ָ������ҵָ���ֱ��滻 */
	DATA bm_stock indus_stock ind_stock;
		SET &stock_pool.;
		IF is_bm = 1 THEN OUTPUT  bm_stock;
		ELSE IF is_bm = 2 THEN OUTPUT  indus_stock;
		ELSE OUTPUT  ind_stock;
	RUN;
	
	/* �Ը����滻��ҵָ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code AS o_code, A.weight AS indus_weight, B.stock_code, B.wt_in_indus
		FROM indus_stock A LEFT JOIN tt_component B
		ON A.end_date = B.end_date AND A.stock_code = B.o_code
		ORDER BY A.end_date, A.stock_code, B.stock_code;
	QUIT;
	DATA indus_stock(drop = o_code indus_weight wt_in_indus);
		SET tmp;
		weight = indus_weight * wt_in_indus;
		is_bm = 0;
		add_in = 1;
	RUN;

	/* �Ը����滻ָ�� */
	PROC SQL;
		CREATE TABLE tt_component AS
		SELECT A.*, A.weight/B.bm_weight AS wt_in_bm
		FROM tt_sub_index A LEFT JOIN 
		(
		SELECT end_date, sum(weight) AS bm_weight
		FROM tt_sub_index
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code AS index_code, A.weight AS index_weight, B.stock_code, B.wt_in_bm
		FROM bm_stock A LEFT JOIN tt_component B
		ON A.end_date = B.end_date
		ORDER BY A.end_date, A.stock_code, B.stock_code;
	QUIT;
	DATA bm_stock(drop = index_code index_weight wt_in_bm);
		SET tmp;
		weight = index_weight * wt_in_bm;
		is_bm = 0;
		add_in = 1;
	RUN;
	DATA ind_stock;
		SET ind_stock;
		add_in = 0;
	RUN;

	DATA &output_stock_pool.;
		SET ind_stock bm_stock indus_stock;
		weight = round(weight, 0.00001);
		IF abs(weight)>=0.00001;
	RUN;
	PROC SORT DATA = &output_stock_pool.;
		BY end_date descending add_in  weight;
	RUN;

	PROC SQL;
		DROP TABLE tt_sub_index, tt_component, ind_stock, bm_stock, indus_stock, tmp;
	QUIT;

%MEND;

/*** ģ��4: ���ݵ�����������ÿ��Ĺ�Ʊ�� */
/** ע��: ����date��ʾ����effective_date������Ʊ����һ���ڳ��еĹ�Ʊ */

/** ������: adjust_date��adjust_weight **/

%MACRO gen_daily_pool(stock_pool, test_period_table, adjust_date_table, output_stock_pool );
	/* Step1: ȷ�����ڵ������� */
	PROC SORT DATA = &adjust_date_table.;
		BY descending end_date;
	RUN;
	DATA tt_adjust;
		SET &adjust_date_table.;
		next_adj_date = lag(end_date);
		FORMAT next_adj_date mmddyy10.;
	RUN;
	
	/* Step2: ȷ����Ӧ�ĵ����� */
	/** ��4�����:
	(1): �ز��ڵĵ�һ��<=��������� --> ֻ�������������֮��Ļز�ʱ�䡣
	(2): �ز��ڵĵ�һ��>��������գ�����ΪƵ�ʲ�ͬ�������������֮��ļ������1�������� --> Ϊ����wt��������⣬�ѻز�����ǰ�ӳ����պþ������������һ��������
	(3): �ز��ڵĵ�һ��ǡ����ĳ�������յ���һ�졣  --> ����
	(4): �ز��ڵ����һ���������������  --> ���ݴ��������ݲ�����
	**/ 
	/** ȡ��ǰ�����һ���ز�������ĵ�����(����)����Ϊ�ز�Ŀ�ͷ��Ϊ�˺������weight��׼ȷ�� **/
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT date
		FROM &test_period_table.
		WHERE today() > date > (SELECT min(end_date) FROM &adjust_date_table.);  /* ��ֻ�������������֮�� */
		
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


	/* Step3: �������һ��������֮��Ľ������� */
	PROC SQL NOPRINT;
		SELECT max(end_date) INTO :adjust_ending
		FROM &adjust_date_table.
	QUIT;
	DATA tt_date_list;
		SET tt_date_list;
		IF missing(adjust_date) THEN adjust_date = &adjust_ending.;
	RUN;


	/* Step4: ���Ʊ������ */
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

/** ģ��5: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�ð汾����ÿ���������) **/
/** daily_stock_pool: ����adjust_weight��adjust_date **/
/** �����й�wt���ֶ�:
(1) ����: open_wt
(2) ����: close_wt
(3) ǰ��Ȩ��(�Ǳ��룬Ϊ������Ʊ׼��)��pre_close_wt
(4) ���̺�Ȩ��(�Ǳ��룬Ϊ�޳���Ʊ׼��): after_close_wt
**/
/** �����������ֶ�: daily_ret/accum_ret/pre_accum_ret(��adjust_date��ʼ) **/
/** ���������ֶ�: pre_date/pre_price/price **/


%MACRO cal_stock_wt_ret(daily_stock_pool, adjust_date_table, output_stock_pool);
	/* Step1: ���㵥�������ʣ��ӵ�����������ۼ������ʵ� */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* ���Ч�ʣ���ȡ�Ӽ�*/
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
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price,
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
		accum_ret = (price/adjust_price - 1)*100;
		pre_accum_ret = (pre_price/adjust_price-1)*100;
		daily_ret = (price/pre_price - 1)*100;  
	/*	daily_ret = (price/pre_close-1)*100;   ����ȫ������㷨 */
	RUN;
	
	/* Step2: �������Ȩ�� */
	/* Step2-1: ����Ȩ�أ�δ����ǰ��*/
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, round((A.adjust_weight*(1+A.accum_ret/100))/B.port_accum_ret,0.00001) AS close_wt
		FROM tt_summary_stock A LEFT JOIN
		(
		SELECT date, sum(adjust_weight*accum_ret/100)+1 AS port_accum_ret
		FROM tt_summary_stock 
		GROUP BY date
		) B
		ON A.date = B.date
		ORDER BY A.date, close_wt desc;
	QUIT;
	/* Step2-2: ����Ȩ�أ��ѵ������Լ�ǰ����Ȩ�� */
	/* �жϸ����ǰһ���Ƿ�Ϊ�����ջ��һ�죬����ǣ�����ʱΪadjust_weight������Ϊǰһ�������Ȩ�� */
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT A.*, B.end_date AS adj_date_b, C.close_wt AS pre_close_wt
		FROM tt_stock_wt A LEFT JOIN &adjust_date_table. B
		ON A.pre_date = B.end_date
		LEFT JOIN tt_stock_wt C
		ON A.pre_date = C.date AND A.stock_code = C.stock_code
		ORDER BY A.date, A.close_wt desc;

/*		SELECT min(date) INTO :first_date*/
/*		FROM tmp;*/
	QUIT;
	DATA tmp(drop = adj_date_b);
		SET tmp;
		IF not missing(adj_date_b) THEN open_wt = adjust_weight; 
		ELSE open_wt = pre_close_wt;
		IF missing(pre_close_wt) THEN pre_close_wt = 0;  /* �����Ĺ�Ʊ */
	RUN;
	/* Step2-3�����̺�����˵�Ȩ�� */
	/* ��һ��Ŀ���Ȩ�� */
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, B.open_wt AS after_close_wt, B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA tt_stock_wt(drop = date_next);
		SET tt_stock_wt;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* ɾ���Ĺ�Ʊ���������: ���һ����Ϊ������ */
	RUN;
	DATA &output_stock_pool.;
		SET tt_stock_wt;
	RUN;
	PROC SQL;
		DROP TABLE tt, tt_hqinfo, tt_summary_stock, tt_stock_wt, tmp;
	QUIT;
%MEND cal_stock_wt_ret;
		
	
/** ģ��5�ȶ�: ��ģ�����ڲ���ģ��5���߼�����ȷ�ġ�����ÿ�յ����ķ��� **/
%MACRO cal_stock_wt_ret_loop(daily_stock_pool, output_stock_pool);
	/* Step1: ȡ����������������һ�������� (���������Ч��) */
	PROC SQL;
		CREATE TABLE effect_date_list AS
		SELECT adjust_date, min(date) AS effective_date  /* ���������û���κι�Ʊ����һ�콫�ᱻ���� */
		FROM &daily_stock_pool. 
		GROUP BY adjust_date
		ORDER BY adjust_date;
	QUIT;
	
	/* Step2: ���㵥�������ʣ��ӵ�����������ۼ������ʵ� */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* ���Ч�ʣ���ȡ�Ӽ�*/
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
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price,
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
		accum_ret = (price/adjust_price - 1)*100;
		pre_accum_ret = (pre_price/adjust_price-1)*100;
		daily_ret = (price/pre_price - 1)*100;  
	RUN;
	
	/* Step3: ����ÿֻ��Ʊÿ���µ�Ȩ�� */
	/** ÿ��ѭ������ **/
	PROC SQL NOPRINT;
		CREATE TABLE tt AS
		SELECT distinct date 
		FROM &daily_stock_pool.
		ORDER BY date;

		SELECT date, count(*) 
		INTO :date_list separated by ' ',
			 :date_number
		FROM tt;
	QUIT;
	
	/* ��ʱ��Ʊ�أ�����ǰһ���Ʊ��close_wt */
	DATA tt_pre_pool;
		ATTRIB
			stock_code LENGTH = $ 16
			date LENGTH = 8 FORMAT = mmddyy10.
			close_wt LENGTH = 8
		;
		STOP;
	RUN;
	/* ���¹�Ʊ��: ���䵱���open_wt, close_wt */
	DATA tt_append_pool;
		ATTRIB
			date LENGTH = 8 FORMAT = mmddyy10.
			stock_code LENGTH = $ 16
			close_wt LENGTH = 8
			open_wt LENGTH = 8
			pre_close_wt LENGTH = 8
		;
		STOP;
	RUN;

	%DO date_index = 1 %TO &date_number.;
		%LET curdate = %scan(&date_list, &date_index., ' ');
		/* Step3-1: ȡ�����ǰһ���Ʊ�� */
		DATA tt_cur_pool;
			SET tt_summary_stock;
			IF date = input("&curdate.", mmddyy10.);
		RUN;
		PROC SQL;
			CREATE TABLE tt_cur_pool_2 AS
			SELECT A.*, B.close_wt AS pre_close_wt
			FROM tt_cur_pool A LEFT JOIN tt_pre_pool B
			ON A.stock_code = B.stock_code
			ORDER BY stock_code;
		QUIT;
		
		/* Step3-2: �жϵ����Ƿ�Ϊ��һ����Ч�� */
		 PROC SQL NOPRINT;
		 	SELECT count(*) INTO :is_effect
			FROM tt_cur_pool_2
			WHERE date IN (SELECT effective_date FROM effect_date_list);
		QUIT;
 		DATA tt_cur_pool;
			SET tt_cur_pool_2;
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
			IF &is_effect. > 0 OR &date_index. = 1 THEN open_wt = adjust_weight; /* ��Ч��: ����Ȩ��Ϊ�������ֵ�Ȩ�� */
			ELSE open_wt = pre_close_wt; /* ����Ϊǰһ������Ȩ�� */
		RUN;
		/* ���㵱�������Ȩ�� */
		PROC SQL NOPRINT;
			SELECT sum(open_wt*(1+daily_ret/100)) INTO :sum_wt
			FROM tt_cur_pool;
		QUIT;
		DATA tt_cur_pool;
			SET tt_cur_pool;
			close_wt = round(open_wt*(1+daily_ret/100)/&sum_wt.,0.00001);
		RUN;
		
		/* Step3-3: ���¹�Ʊ�� */
		DATA tt_append_pool;
			SET tt_append_pool tt_cur_pool(keep = date stock_code close_wt open_wt pre_close_wt);
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
		RUN;
		DATA tt_pre_pool;
			SET tt_cur_pool(keep = date stock_code close_wt);
		RUN;
	%END;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close_wt, B.open_wt, B.pre_close_wt
		FROM tt_summary_stock A LEFT JOIN tt_append_pool B
		ON A.date = B.date AND A.stock_code = B.stock_code
		ORDER BY A.date, B.close_wt desc;
	QUIT;
	
	/* ���̵�����Ȩ�� */
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT A.*, B.open_wt AS after_close_wt, B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET &output_stock_pool.;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* ɾ���Ĺ�Ʊ */
	RUN;

	PROC SQL;
		DROP TABLE  effect_date_list, tt, tt_hqinfo, tt_summary_stock, tt_pre_pool, tt_append_pool, tt_cur_pool, tt_cur_pool_2, tmp;
	QUIT;
%MEND cal_stock_wt_ret_loop;


/** ģ��6: ������ϵ������alpha�� */
/* ��ϵĵ��������ṩ���ּ��㷽��: һ�������õ�����ϵ��ۻ����棬���Ƽ��㣻��һ����ֱ�����õ������weightֱ�Ӽ�Ȩ�������档�ȶԶ���Ӧ������ͬ�� */
%MACRO cal_portfolio_ret(daily_stock_pool, output_daily_summary);
	/* �����ս���ͳ�� */
	PROC SQL;
		CREATE TABLE tt_summary_day AS
		SELECT date, 
			sum(open_wt*daily_ret) AS daily_ret,
			((sum(adjust_weight*accum_ret/100)+1)/(sum(adjust_weight*pre_accum_ret/100)+1)-1)*100 AS daily_ret_p,  /* �������֪��daily_ret��daily_ret_p����ͬ�� */
			sum(1-add_in) AS tar_nstock, count(1) AS nstock
		FROM &daily_stock_pool.
		GROUP BY date;
	QUIT;

	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+daily_ret/100)-1)*100;
	RUN;
	
	/* ����ָ���������� */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE tt_bm_hqinfo AS
		SELECT A.stock_code AS index_code, A.end_date AS date, A.close, 
			E.pre_date AS pre_date, B.close AS pre_close, (A.close/B.close-1)*100 AS bm_daily_ret
		FROM benchmark_hqinfo A LEFT JOIN tt E
		ON A.end_date = E.date
		LEFT JOIN benchmark_hqinfo B
		ON E.pre_date = B.end_date
		WHERE A.end_date IN 
		(SELECT date FROM tt_summary_day)
		ORDER BY A.end_date;
	QUIT;
	
	/* ������ϵ�alpha���ۻ�alpha */
	PROC SQL;
		CREATE TABLE tmp  AS
		SELECT A.*, B.bm_daily_ret
		FROM tt_summary_day A LEFT JOIN tt_bm_hqinfo B
		ON A.date = B.date
		ORDER BY A.date;
	QUIT;
	DATA &output_daily_summary.;
		SET tmp;
		RETAIN bm_accum_ret 0;
		RETAIN accum_alpha 0;
		bm_accum_ret = ((bm_accum_ret/100+1)*(1+bm_daily_ret/100)-1)*100;
		alpha = daily_ret - bm_daily_ret;
		accum_alpha = ((accum_alpha/100+1)*(1+alpha/100)-1)*100;
		/* ����Ϊָ�� */
		index = 100 * (1+accum_ret/100);
		bm_index = 100 * (1+bm_accum_ret/100);
		alpha_index = 100*(1+accum_alpha/100);
		/* ʤ�� */
		IF alpha > 0 THEN is_hit = 1;
		ELSE is_hit = 0;
	RUN;
	PROC SQL;
		DROP TABLE tt_summary_day, tt, tmp, tt_bm_hqinfo;
	QUIT;
%MEND cal_portfolio_ret;


/** ģ��7: �����嵥�����������ʣ�**/
/** �������ű�: ��Ϊ���׷�����date�����̺�
(1) ��Ʊ�����嵥; ����date/stock_code/initial_wt/traded_wt/final_wt/status/trade_type/trans_cost(�ݶ�Ϊ����0.35%)
(2) ÿ�콻���嵥: ����date/delete_assets/added_assets/sell_wt/buy_wt/buy_cost/sell_cost/turnover(˫�ߺ�)
**/
%MACRO trading_summary(daily_stock_pool, adjust_date_table, output_stock_trading, output_daily_trading);
	
	/* Step1: ֻ���ǵ����պ͵�����֮��ļ�¼ */
	PROC SQL;
		CREATE TABLE tt_stock_pool AS
		SELECT date, pre_date, stock_code, close_wt, open_wt, pre_close_wt, after_close_wt, add_in
		FROM &daily_stock_pool.
		WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
		OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
	QUIT;
	/* Step2: ȷ����Ʊ�䶯*/
	DATA tt_stock_pool;
		SET tt_stock_pool;
		/* ���� */
		IF after_close_wt = 0 THEN status = -1;  /* ��ʾ�޳� */
		ELSE status = 0; /* ���� */
		initial_wt = close_wt;
		traded_wt = after_close_wt - close_wt;
		final_wt = after_close_wt;
		IF after_close_wt - close_wt > 0 THEN trade_type = 1;
		ELSE IF after_close_wt = close_wt THEN trade_type = 0;
		ELSE trade_type = -1;
		/* ǰһ�� */
		IF pre_close_wt = 0 THEN status_p = 1;
		ELSE status_p = 0;
		initial_wt_p = pre_close_wt;
		traded_wt_p = open_wt - pre_close_wt;
		final_wt_p = open_wt;
		IF open_wt - pre_close_wt > 0 THEN trade_type_p = 1;
		ELSE IF open_wt = pre_close_wt  THEN trade_type_p = 0;
		ELSE trade_type_p = -1;
	RUN;

	/* Step3�������еĵ��ֶ���Ϊ���������̺� */
	/* ȷ�������������еĵ����� */
	PROC SQL;
		CREATE TABLE tt_adjust AS
		SELECT distinct adjust_date AS date
		FROM &daily_stock_pool.;
	QUIT;
		
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.date, B.stock_code,
		B.status, B.trade_type, B.initial_wt, B.traded_wt, B.final_wt, B.add_in
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.date /* �ڵ�һ�ε���ʱ, stock_code_b����Ϊ�� */
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tmp2 AS
		SELECT A.date, B.stock_code, B.status_p, B.trade_type_p, 
		B.initial_wt_p, B.traded_wt_p, B.final_wt_p, B.add_in
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.pre_date
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tt_stock_pool AS  /* ȡ��������Ʊ���û�е�������ģ���tmp1��tmp2����һ���� */
		SELECT *
		FROM tmp1 UNION
		(SELECT date, stock_code, status_p AS status, trade_type_p AS trade_type,
		initial_wt_p AS initial_wt, traded_wt_p AS traded_wt, final_wt_p AS final_wt, add_in
		FROM tmp2) 
		ORDER BY date, stock_code;
	QUIT;
	DATA &output_stock_trading.;
		SET tt_stock_pool;
		trans_cost = abs(traded_wt) * 0.0035;
	RUN;

	/* Step4: ͳ��ÿ������ */
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


/** ģ��8: �������� */
%MACRO eval_pfmance(daily_summary, daily_trading, test_period_table, output_daily_summary, output_pfmance_summary);
		/* �ز�����ͳ�� */
		PROC SQL;
			CREATE TABLE tt_summary_day AS
			SELECT A.*, B.turnover
			FROM &daily_summary. A LEFT JOIN &daily_trading. B
			ON A.date = B.date
			WHERE A.date IN
			(SELECT date FROM &test_period_table.)   /* ����ʱ��: ɾ��֮ǰ���ӵļ��������գ�ע�������Ͳ��ῼ�ǵ�һ�ν���Ļ��� */
			ORDER BY A.date;
		QUIT;
		DATA tt_summary_day;
			SET tt_summary_day;
			RETAIN accum_alpha_tc 0;
			IF missing(turnover) THEN turnover = 0;
			alpha_tc = alpha - turnover * &trans_cost.;
			accum_alpha_tc = ((1+accum_alpha_tc/100)*(1+alpha_tc/100)-1)*100;
			alpha_tc_index = 100*(1+accum_alpha_tc/100);
		RUN;
		DATA &output_daily_summary.;
			SET tt_summary_day;  /* ����turnover��*/
		RUN;

		DATA tt_summary_day(drop = max_bm max_index max_alpha max_alpha_tc);
			SET tt_summary_day;
			year = year(date);
			month = month(date);
			RETAIN max_bm .;
			RETAIN max_index .;
			RETAIN max_alpha .;
			RETAIN max_alpha_tc .;

			/* �������س� */
			IF bm_index >= max_bm THEN max_bm = bm_index;
			bm_draw = (bm_index - max_bm)/max_bm * 100;
			IF index >= max_index THEN max_index = index;
			index_draw = (index - max_index)/max_index *100;
			IF alpha_index >= max_alpha THEN max_alpha = alpha_index;
			alpha_draw = (alpha_index - max_alpha)/max_alpha *100;
			IF alpha_tc_index >= max_alpha_tc THEN max_alpha_tc = alpha_tc_index;
			alpha_tc_draw = (alpha_tc_index - max_alpha_tc)/max_alpha_tc *100;
		RUN;

		/* ����ȼ������س����ۼ�alpha����׼�ۼ����棬ָ���ۼ����� */
		DATA tt_summary_day(drop = max_bm max_index max_alpha max_alpha_tc);
			SET tt_summary_day;
			BY year;
			RETAIN max_bm .;
			RETAIN max_index .;
			RETAIN max_alpha . ;
			RETAIN max_alpha_tc .;
			RETAIN accum_ret_year 0;
			RETAIN accum_bm_ret_year 0;
			RETAIN accum_alpha_year 0;
			RETAIN accum_alpha_tc_year 0;

			IF first.year THEN DO;
				max_bm = .;
				max_index = .;
				max_alpha = .;
				max_alpha_tc = .;
				accum_ret_year = 0;
				accum_bm_ret_year = 0;
				accum_alpha_year = 0;
				accum_alpha_tc_year = 0;
			END;

			/* �������س� */
			IF bm_index >= max_bm THEN max_bm = bm_index;
			bm_draw_year = (bm_index - max_bm)/max_bm * 100;
			IF index >= max_index THEN max_index = index;
			index_draw_year = (index - max_index)/max_index *100;
			IF alpha_index >= max_alpha THEN max_alpha = alpha_index;
			alpha_draw_year = (alpha_index - max_alpha)/max_alpha *100;
			IF alpha_tc_index >= max_alpha_tc THEN max_alpha_tc = alpha_tc_index;
			alpha_tc_draw_year = (alpha_tc_index - max_alpha_tc)/max_alpha_tc *100;
			
			/* �ۼ�alpha����׼�ۼ����棬ָ���ۼ����� */
			accum_ret_year  = ((1+accum_ret_year/100)*(1+daily_ret/100)-1)*100;
			accum_bm_ret_year  = ((1+accum_bm_ret_year/100)*(1+bm_daily_ret/100)-1)*100;
			accum_alpha_year = ((1+accum_alpha_year/100)*(1+alpha/100)-1)*100;
			accum_alpha_tc_year = ((1+accum_alpha_tc_year/100)*(1+alpha_tc/100)-1)*100;
		RUN;

	 /* ����ָ�� */
	/* �ۼ������� + ��׼������ + �ۻ�alpha + alpha������ + IR + ʤ�� + ƽ���ֱֲ��� + ������ + ���س�(%) */
 
		/* ������� */
		DATA stat1(rename = (accum_ret_year = accum_ret accum_bm_ret_year = accum_bm_ret accum_alpha_year = accum_alpha
						accum_alpha_tc_year = accum_alpha_tc));
			SET tt_summary_day(keep = year accum_ret_year accum_bm_ret_year accum_alpha_year accum_alpha_tc_year tar_nstock nstock);
			BY year;
			IF last.year;
		RUN;
		PROC SQL;
			CREATE TABLE stat2 AS
			SELECT year,
			sqrt(var(alpha))*sqrt(250) AS sd_alpha,
			sum(alpha)*sqrt(250)/(count(1)*sqrt(var(alpha))) AS ir,
			sqrt(var(alpha_tc))*sqrt(250) AS sd_alpha_tc,
			sum(alpha_tc)*sqrt(250)/(count(1)*sqrt(var(alpha_tc))) AS ir_tc,
			mean(is_hit) AS hit_ratio,
			sum(turnover) AS turnover,
			min(index_draw_year) AS index_draw,
			min(bm_draw_year) AS bm_draw,
			min(alpha_draw_year) AS alpha_draw,
			min(alpha_tc_draw_year) AS alpha_tc_draw,
			count(1) AS ndays,
			mean(tar_nstock) AS m_tar_nstock,
			mean(nstock) AS m_nstock,
			sum(turnover>0) AS n_trade_day
			FROM tt_summary_day
			GROUP BY year;
		QUIT;

		PROC SQL;
			CREATE TABLE tt_summary_stat1 AS
			SELECT A.*, B.*
			FROM stat1 A JOIN stat2 B
			ON A.year = B.year
			ORDER BY A.year;
		QUIT;

		/* �ز��������� */
		DATA stat1;
			SET tt_summary_day(keep = tar_nstock nstock) end = is_end;
			IF is_end = 1;
			year = 0;
		RUN;
		PROC SQL;
			CREATE TABLE stat2 AS
			SELECT 0 AS year,
			mean(alpha)*250 AS accum_alpha, /* �������׼�� */
			mean(alpha_tc)*250 AS accum_alpha_tc, 
			mean(daily_ret)*250 AS accum_ret,
			mean(bm_daily_ret)*250 AS accum_bm_ret,
			sqrt(var(alpha))*sqrt(250) AS sd_alpha,
			sum(alpha)*sqrt(250)/(count(1)*sqrt(var(alpha))) AS ir,
			sqrt(var(alpha_tc))*sqrt(250) AS sd_alpha_tc,
			sum(alpha_tc)*sqrt(250)/(count(1)*sqrt(var(alpha_tc))) AS ir_tc,
			mean(is_hit) AS hit_ratio,
			mean(turnover)*250 AS turnover,  /* �껯���� */
			min(index_draw) AS index_draw,
			min(bm_draw) AS bm_draw,
			min(alpha_draw) AS alpha_draw,
			min(alpha_tc_draw) AS alpha_tc_draw,
			count(1) AS ndays,
			mean(tar_nstock) AS m_tar_nstock,
			mean(nstock) AS m_nstock,
			sum(turnover>0) AS n_trade_day
			FROM tt_summary_day
		QUIT;
		PROC SQL;
			CREATE TABLE tt_summary_stat2 AS
			SELECT A.*, B.*
			FROM stat1 A JOIN stat2 B
			ON A.year = B.year
			ORDER BY A.year;
		QUIT;
		
		DATA tt_summary_stat(drop = i);
			SET tt_summary_stat1 tt_summary_stat2;
			ARRAY var_a(15) accum_ret accum_bm_ret accum_alpha accum_alpha_tc
				sd_alpha ir sd_alpha_tc ir_tc turnover bm_draw index_draw alpha_draw alpha_tc_draw 
				m_tar_nstock m_nstock;
			DO i = 1 TO 15;
				var_a(i) = round(var_a(i),0.01);
			END;
			hit_ratio = round(hit_ratio,0.0001);
		RUN; 

		PROC TRANSPOSE DATA = tt_summary_stat  OUT = &output_pfmance_summary.
			prefix = Y_  name = stat1;
			id year;
		RUN;

		
		PROC SQL;
			DROP TABLE tt_summary_stat1, tt_summary_stat2, stat1, stat2, tt_summary_day, tt_summary_stat;
		QUIT;

%MEND eval_pfmance;


/** ģ��9: ��¶�ȷ����������ڲ�ͬ����Լ���ҵ�ϵı�¶�� **/	
/** �ⲿ��: index_component **/
%MACRO exposure_analyze(daily_stock_pool, output_daily_exposure_t);
	/*Step1: ����С�壬��ҵ�壬�Լ�300/500�ı�¶�� */
	DATA tmp;
		SET &daily_stock_pool.;
		IF index(stock_code,"002") = 1 THEN zxb = 1;
		ELSE zxb = 0;
		IF index(stock_code,"300") = 1 THEN cyb = 1;
		ELSE cyb = 0;
	RUN;
	PROC SQL;
		CREATE TABLE tt_stock_pool AS
		SELECT A.stock_code, A.date, A.pre_date, 
			B.stock_code AS stock_code_b, B.index_code
		FROM tmp A LEFT JOIN index_component B
		ON A.stock_code = B.stock_code AND A.pre_date = B.end_date 
		WHERE index_code = "000300"
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tt_stock_pool(drop = index_code stock_code_b);
		SET tt_stock_pool;
		IF stock_code = stock_code_b AND index_code = "000300" THEN hs300 = 1;
		ELSE hs300 = 0;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.hs300
		FROM tmp A LEFT JOIN tt_stock_pool B
		ON A.stock_code = B.stock_code AND A.date = B.date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tmp;
		SET tmp2;
		IF missing(hs300) THEN hs300 = 0;
	RUN;

	PROC SQL;
		CREATE TABLE tt_stock_pool AS
		SELECT A.stock_code, A.date, A.pre_date, 
			B.stock_code AS stock_code_b, B.index_code
		FROM tmp A LEFT JOIN index_component B
		ON A.stock_code = B.stock_code AND A.pre_date = B.end_date 
		WHERE index_code = "000905"
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tt_stock_pool(drop = index_code stock_code_b);
		SET tt_stock_pool;
		IF stock_code = stock_code_b AND index_code = "000905" THEN zz500 = 1;
		ELSE zz500 = 0;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.zz500
		FROM tmp A LEFT JOIN tt_stock_pool B
		ON A.stock_code = B.stock_code AND A.date = B.date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tt_stock_pool;
		SET tmp2;
		IF missing(zz500) THEN zz500 = 0;
	RUN;

	/** Step2: ����ҵ�ϵı�¶�� **/
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.o_code
		FROM tt_stock_pool A LEFT JOIN stock_sector_mapping B
		ON A.stock_code = B.stock_code AND A.pre_date = B.end_date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tt_stock_pool;
		SET tmp;
	RUN;
	/** ������ҵ���ƺ�������ں������ */
	PROC SQL;
		CREATE TABLE tt_industry_code AS
		SELECT distinct(o_code) AS o_code
		FROM tt_stock_pool
		ORDER BY o_code;
	QUIT;

	DATA _null_;
		SET tt_industry_code end = last;
		CALL symput(compress("var"||_N_),o_code);
		IF last THEN CALL symput("countj",_N_);
	RUN;
	%DO j = 1 %TO &countj.;
		DATA tt_stock_pool;
			SET tt_stock_pool;
			IF strip(o_code)=trim("&&var&j.") THEN &&var&j.=1;
			ELSE &&var&j.=0;
		RUN;
	%END;
	
	/* Step3: ͳ��ÿ��ɷֹɣ���ҵ�ϵ������ʹ��� */
	/* �ų���Ϊ��Ȩ�ȣ�������ĳɷֹ� */
	%LET var_list = hs300-zz500-zxb-cyb;
	%LET var_list2 = ;
	%DO j = 1 %TO &countj.;
		%LET var_list2 = %SYSFUNC(catx(-, &var_list2.,&&var&j. ));
	%END;
	%LET var_list = %SYSFUNC(catx(-, &var_list., &var_list2.));
	
	PROC SQl;
		CREATE TABLE tt_summary AS
		SELECT distinct date
		FROM tt_stock_pool
		ORDER BY date;
	QUIT;
	
	%DO var_index = 1 %TO 24;
		%LET curvar = %scan(&var_list., &var_index., -);
		PROC SQL;
			CREATE TABLE tmp_&var_index. AS
			SELECT date, sum(&curvar.) AS &curvar._nstock,
			sum(close_wt * &curvar.) AS &curvar._wt,
			sum(open_wt * &curvar. * daily_ret) AS &curvar._ret
			FROM tt_stock_pool
			WHERE add_in = 0
			GROUP BY date;
		QUIT;
		PROC SQL;
			CREATE TABLE tmp_all AS
			SELECT A.*, B.&curvar._nstock, B.&curvar._wt, B.&curvar._ret
			FROM tt_summary A LEFT JOIN tmp_&var_index. B
			ON A.date = B.date
			ORDER BY A.date;
		QUIT;
		DATA tt_summary;
			SET tmp_all;
		RUN;
		PROC SQL;
			DROP TABLE tmp_&var_index.;
		QUIT;
	%END;
	
	
	DATA &output_daily_exposure_t.;
		SET tt_summary;
		%DO var_index = 1 %TO %EVAL(&countj.+ 4);
			%LET curvar = %scan(&var_list., &var_index., -);
			IF missing(&curvar._nstock) THEN  &curvar._nstock = 0;
			IF missing(&curvar._wt) THEN &curvar._wt = 0;
			IF missing(&curvar._ret) THEN &curvar._ret = 0;
		%END;
	RUN;
	
	PROC SQL;
		DROP TABLE tmp, tt_stock_pool, tt_industry_code, tt_summary;
	QUIT;
%MEND exposure_analyze;

%MACRO cal_holding_list(stock_trading_table, test_period_table, output_holding_list);
	/* ѡ����������� */
	PROC SQL NOPRINT;
		SELECT max(date) INTO :end_date
		FROM &test_period_table.;
	QUIT;

	PROC SORT DATA = &stock_trading_table. OUT = tt_stock_trading;   /** ����ļ�¼��ֻ�а����������ڵļ�¼ **/
		BY stock_code date;
	RUN;
	
	/** ��ȡ��Ʊ���� */

/*	PROC SQL;*/
/*		CREATE TABLE tmp AS*/
/*		SELECT A.*, B.ob_object_name_1090 AS stock_name*/
/*		FROM tt_stock_trading A LEFT JOIN locwind.TB_OBJECT_1090 B*/
/*		ON A.stock_code = B.f16_1090 AND B.f4_1090 = "S"*/
/*		ORDER BY A.stock_code, A.date;*/
/*	QUIT;*/
/*	DATA tt_stock_trading;*/
/*		SET tmp;*/
/*	RUN;*/

	
	/* ÿֻ��Ʊ��ÿ��������¼ */
	DATA tt_stock_trading;
		SET tt_stock_trading;
		BY stock_code;
		RETAIN buy_date .;
		RETAIN t_initial_wt .;
		RETAIN trading_days 0;   /** �����˼��ε��� **/
		RETAIN t_trans_cost 0;
		IF first.stock_code OR status = 1 THEN DO;  /* ���������ź� */
			buy_date = date;
			trading_days = 0;
			t_initial_wt = final_wt;  /* ��������ʱ���final_wt */
			t_trans_cost = 0;
		END;
		ELSE DO;
			trading_days + 1;
		END;
		FORMAT buy_date mmddyy10.;
		t_trans_cost + trans_cost;
	RUN;
	DATA tt_stock_trading(keep = stock_code date buy_date  t_initial_wt t_final_wt delta_wt trading_days add_in rename = (t_initial_wt = initial_wt t_final_wt = final_wt date = sell_date));
		SET tt_stock_trading;
		IF status = -1 OR date = &end_date.;
		t_final_wt = initial_wt;
		delta_wt = t_final_wt - t_initial_wt;
	RUN;

	/** ȷ�����е����� **/
	%map_date_to_index(busday_table=busday, raw_table=tt_stock_trading, date_col_name=buy_date, raw_table_edit=tt_stock_trading_1);
	DATA tt_stock_trading_1;
		SET tt_stock_trading_1(rename = (date_index = buy_date_index));
	RUN;
	%map_date_to_index(busday_table=busday, raw_table=tt_stock_trading_1, date_col_name=sell_date, raw_table_edit=tt_stock_trading_1);
	DATA tt_stock_trading(drop = date_index buy_date_index);
		SET tt_stock_trading_1;
		holding_days = date_index - buy_date_index;
	RUN;

	
	/* �����ڳ����ڼ���ۼƾ������棬��׼���棬��ҵָ�����棬��Ի�׼��alpha�������ҵ��alpha�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close*B.factor AS buy_price, C.close*C.factor AS sell_price, (C.close*C.factor- B.close*B.factor)/( B.close*B.factor) * 100 AS accum_ret
		FROM tt_stock_trading A
		LEFT JOIN hqinfo B
		ON A.buy_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN hqinfo C
		ON A.sell_date = C.end_date AND A.stock_code = C.stock_code
		ORDER BY add_in, A.buy_date, A.stock_code;
	QUIT;


	
	/* ���׼���� */
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, (C.close- B.close)/( B.close) * 100 AS bm_accum_ret, accum_ret-(C.close- B.close)/( B.close) * 100 AS accum_alpha
		FROM tmp A
		LEFT JOIN benchmark_hqinfo B
		ON A.buy_date = B.end_date
		LEFT JOIN benchmark_hqinfo C
		ON A.sell_date = C.end_date
		ORDER BY add_in, A.buy_date, A.stock_code;
	QUIT;
	
	/* ����ҵָ������ */
/*	PROC SQL;*/
/*		CREATE TABLE tmp3 AS*/
/*		SELECT A.*, D.o_code, D.o_name,  (C.close- B.close)/( B.close) * 100 AS index_accum_ret, accum_ret-(C.close- B.close)/( B.close) * 100 AS accum_index_alpha*/
/*		FROM tmp2 A LEFT JOIN stock_sector_mapping D*/
/*		ON A.stock_code = D.stock_code AND A.buy_date = D.end_date*/
/*		LEFT JOIN index_hqinfo B*/
/*		ON A.buy_date = B.end_date AND D.o_code = B.stock_code*/
/*		LEFT JOIN index_hqinfo C*/
/*		ON A.sell_date = C.end_date AND D.o_code = C.stock_code*/
/*		ORDER BY add_in, A.buy_date, A.stock_code;*/
/*	QUIT;*/
	
/*	DATA &output_holding_list.;*/
/*		SET tmp3;*/
/*	RUN;*/
	DATA &output_holding_list.;
		SET tmp2;
	RUN;


	PROC SQL;
		DROP TABLE  tt_stock_trading, tmp, tmp2, tmp3, tt_stock_trading_1;
	QUIT;

%MEND;

