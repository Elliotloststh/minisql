#include<iostream>
#include<string>
#include"interpreter.hpp"
#include"API.hpp"

Buffer bm;
CatalogManager CL;
IndexManager IM;
RecordManager RM;

using namespace BufferManager;

int main()
{
	InterPreter temp;

	while (1)
	{
		try
		{
			cout << "Minisql>";
			string sql = temp.Read_input();
			SqlCommand c = temp.Final_expression(sql);

			API api(c);
			api.execute(c);
		}
		catch (CatalogError e)
		{
			cout << e.what() << endl;
		}
	}
	system("pause");

	return 0;
}







