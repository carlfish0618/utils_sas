/** ======================˵��=================================================**/
/*** �������ܣ��ṩ�������޹صĸ�����ͨ�ú��� **** /

/**** �����б�:
(1) get_sector_info: ��ȡ��ҵ��Ϣ
(2) get_stock_size: ��ȡ��ֵ����ͨ��ֵ��Ϣ��
(3) read_from_excel: ��Excel�ж�ȡ�ļ�
(4) output_to_excel: ����ļ���Excel��
(5) plot_normal: ����̬ͼ
(6) cal_coef: �������ϵ��(����pearson��spearman)
****/ 

/** =======================================================================**/


options validvarname=any; /* ֧�����ı����� */

/** ģ��1: ��ȡ��ҵ��Ϣ */
/** ����:
(1) stock_table: stock_code/end_date/����
(2) mapping_table: stock_code/end_date/indus_code/indus_name  */

/** ���:
(1) output_stock_table: end_date/stock_code/indus_code/indus_name/����  **/

%MACRO get_sector_info(stock_table, mapping_table, output_stock_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM &stock_table. A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND A.end_date > B.end_date  AND A.end_date <= B.end_date + 100 /* ���ȡ100��͹��� */
		GROUP BY A.end_date, A.stock_code
		HAVING B.end_date = max(B.end_date) 
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	/** �����ҵ��Ϣȱʧ���п������¹ɣ���δ������ҵ��Ϣ����ʱ�����ѷ����ģ���һ����¼����ҵ���� */
	PROC SQL;
		CREATE TABLE miss_subset AS
		SELECT stock_code, end_date
		FROM tmp 
		WHERE missing(indus_code);
	QUIT;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM miss_subset A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND B.end_date >= A.end_date
		GROUP BY A.stock_code, A.end_date
		HAVING B.end_date = min(B.end_date)
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	/** ���� */
	DATA tmp;
		UPDATE tmp tmp2;
		BY end_date stock_code;
	RUN;

	DATA &output_stock_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tmp2, miss_subset;
	QUIT;

%MEND get_sector_info;


/** ģ��2: ��ȡ������Ϣ */
/** ����:
(1) stock_table: stock_code/end_date/����
(2) index(numeric): ����ȡ 1-freeshare 2-a_share 3-total_share 4-liqa_share��һ�ߡ�
(3) info_table: stock_code/end_date/close
(4) share_table: stock_code/end_date/freeshare(a_share,total_share,liqa_share��) 
(5) colname: �������ֵ�н���������

/** ���:
(1) output_stock_table: end_date/stock_code/value  **/
%MACRO get_stock_size(stock_table, info_table, share_table,output_table, colname, index = 1);
	%IF %SYSEVALF(&index. = 1) %THEN %DO;
		%LET var_name = freeshare;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 2) %THEN %DO;
		%LET var_name = a_share;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 3) %THEN %DO;
		%LET var_name = total_share;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 4) %THEN %DO;
		%LET var_name = liqa_share;
	%END;
	

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close, C.&var_name.
		FROM &stock_table. A LEFT JOIN &info_table. B
		ON A.stock_code = B.stock_code AND A.end_Date = B.end_date
		LEFT JOIN &share_table. C
		ON A.stock_code = C.stock_code AND A.end_date = C.end_Date
		ORDER BY A.end_Date, A.stock_code;
	QUIT;
	DATA &output_table.(drop = close &var_name.);
		SET tmp;
		&colname. = close * &var_name.;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND get_stock_size;

/** ģ��3�� ���ⲿ����excel�ļ� */
%MACRO read_from_excel(excel_path, output_table, sheet_name = Sheet1$);
	PROC IMPORT OUT = &output_table.
            DATAFILE= "&excel_path." 
            DBMS=EXCEL REPLACE;
     	RANGE="&sheet_name."; 
     	GETNAMES=YES;
     	MIXED=NO;
     	SCANTEXT=NO;
     	USEDATE=YES;
     	SCANTIME=YES;
	RUN;
%MEND read_from_excel;


/** ģ��4�������excel�ļ��� */
/** ������replace */
%MACRO output_to_excel(excel_path, input_table, sheet_name = data);
	LIBNAME myxls "&excel_path.";  /* external file */
		DATA myxls.&sheet_name.;
			SET &input_table.;
		RUN;
	LIBNAME myxls CLEAR;
%MEND output_to_excel;

/** ģ��5: ����̬ͼ */
%MACRO plot_normal(var,data);
	proc univariate data=&data. normal; 
    	var &var.;
    	histogram &var.; 
    	probplot &var.;
	run;
%MEND plot_normal;


