grammar OpenDBExpr;

// We define expression to be either a method call or a string.
expression
    : expression DOT NAME
    | THIS
    | methodCall 
    | DOT NAME
    | STRING_LITERAL2
    | STRING_LITERAL1
    | INT
    ;

methodCall  : packageName OPENB expression (COMMA expression)*  CLOSEB;
packageName : NAME (':' NAME)*;


// NAME represents any variable or method name.
THIS : 'this' ;
NAME : [a-zA-Z_][a-zA-Z0-9_]*;
INT : '-'? '0'..'9'+ ;
DOT : '.';
COMMA : ',';
OPENB : '(';
CLOSEB : ')';

// STRING represents a string value, for example "abc".
STRING_LITERAL2 : '"' (~('"' | '\\' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';
STRING_LITERAL1 : '\'' (~('\'' | '\\' | '\r' | '\n') | '\\' ('\'' | '\\'))* '\'';

WS : [ \t\u000C\r\n]+ -> skip ;
