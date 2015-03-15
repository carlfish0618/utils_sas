/** ======================˵��=================================================**/
/*** �������ܣ��ṩ�������޹صĸ�����ͨ�ú��� **** /

/**** �����б�:
(1) get_sector_info: ��ȡ��ҵ��Ϣ
****/ 

/** =======================================================================**/



/** ģ��1: ��ȡ��ҵ��Ϣ */
/** ����:
(1) stock_table: stock_code/end_date/����
(2) mapping_table: stock_code/end_date/indus_code/indus_name  */

/** ���:
(1) output_stock_table: end_date/stock_code/indus_code/indus_name/����  **/

%MACRO get_sector_info(stock_table, mapping_table, output_stock_table);
	PROC SQL;
		CREATE TABLE &output_stock_table. AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM &stock_table. A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date   
		ORDER BY A.end_date, B.indus_code;
	QUIT;

%MEND get_sector_info;
