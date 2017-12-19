/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

grammar sparsecsv;



csv : row (NEWLINE row)* NEWLINE? EOF;
row : entry (col entry?)* | (col entry?)+;  

col : COMMA;
entry : sparse_array
      | string;

sparse_array : QUOTES? LPAREN indices COMMA elements RPAREN QUOTES?;

array : LBRACE? string ((BSCOMMA | COMMA) string)* RBRACE? NEWLINE? EOF;
               
indices : QUOTES? LBRACE (string (COMMA string?)* | (COMMA string?)+)? RBRACE QUOTES?;

elements : QUOTES? LBRACE (string (COMMA string?)* | (COMMA string?)+)? RBRACE QUOTES?;

string : QUOTES? STRING QUOTES? | QUOTES;

LBRACE : '{';
RBRACE : '}';
LPAREN : '(';
RPAREN : ')';
COMMA : ',';
QUOTES : '"'+;
STRING : (~('('|')'|'{'|'}'|'"'|','|'\n'|'\\'))+;
NEWLINE : '\n';
BSCOMMA : '\\,';
