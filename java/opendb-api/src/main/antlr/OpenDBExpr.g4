grammar OpenDBExpr;

// We define expression to be either a method call or a string.
expression
    : methodCall
    | fieldAccess
    | STRING_LITERAL2
    | STRING_LITERAL1
    | INT
    ;

fieldAccess : 'this'? ('.' NAME) + ;
methodCall : NAME '(' methodCallArguments ')' ;

methodCallArguments
	: // No arguments
    | expression (',' expression)*  
    ;

// NAME represents any variable or method name.
NAME : [a-zA-Z][a-zA-Z0-9]*;
INT : '-'? '0'..'9'+ ;

// STRING represents a string value, for example "abc".
STRING_LITERAL2 : '"' (~('"' | '\\' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';
STRING_LITERAL1 : '\'' (~('\'' | '\\' | '\r' | '\n') | '\\' ('\'' | '\\'))* '\'';

WS : [ \t\u000C\r\n]+ -> skip ;
