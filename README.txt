Student: Boldisor Dragos-Alexandru
Grupa: 332CB

            Tema 2 APD

    Clasa Tema2 - clasa care contine functia main. De asemenea, main o sa fie
thread-ul coordonator si el va citi fisierul de input, va impartii taskurile
pentru Workeri si va afisa.
    Interfata MapReduceInterface - implementata de Mapper si Reducer. Folosita
pentru genericitate (T implements MapReduceInterface).
    Clasa generica Pair - care retine doua date de tipuri diferite.
    Clasa Worker - implementeaza Runnable si primeste in constructor, pe langa
task, tipul taskului pe care va urma sa il execute in functia run().
    Clasa Mapper - clasa care reprezinta un task de tip map. Are o metoda map()
care citeste informatia din fisier (prin metoda readInformation()). Apoi creaza
HashMap-ul cu lungimile si numarul cuvintelor precum si ArrayList-ul cu cele
mai lungi cuvinte. Rezultatul este memorat intr-un field si este un pair de
hashmap si arraylist.
    Clasa Reducer - clasa care reprezinta un task de tip reduce.
Are o metoda reduce() care combina toate HashMap-urile si ArrayList-urile
primite ca input (practic rezultatele taskurilor de map) si calculeaza rank-ul
fisierului dupa formula prezentata in enunt. Rezultatul este memorat in 2
field-uri (float rank (folosit pentru afisa descrescator), si string result
care reprezinta string-ul ce trebuie afisat).