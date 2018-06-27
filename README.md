# minisql

由于编译器的区别，visual studio用户（非g++/clang++编译器）请注释掉API.cpp第431行：

```
sql_com = sql_com.substr(0,sql_com.length()-1); 
```

macOS平台可执行文件已编译，在src文件夹下

```
./minisql
```

