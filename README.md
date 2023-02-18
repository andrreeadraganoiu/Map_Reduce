335CB Draganoiu Andreea

Paradigma Map-Reduce

Se citesc fisierele de intrare din fisierul principal si se stocheaza 
intr-un vector. Acestea se trimit intr-o structura ca argument pentru
functia map, respectiv reduce cand sunt create thread-urile.

Functia map - Se alege cate un fisier pentru procesare, moment in care
se scoate din vector. Pentru a nu alege / sterge mai multe thread-uri acelasi
fisier, se pune lock pe regiunea respectiva. Se cauta puterile perfecte folosind
cautarea binara in stanga, respectiv in dreapta bazei numarului citit. Pentru a nu
calcula de mai multe ori ridicarea la putere, valoarea se stocheaza intr-o 
variabila. Pentru puterile foarte mari, se intampla overflow, numarul devenind 
negativ. Puterile pentru fiecare exponent sunt retinute intr-un vector de seturi
locale pentru fiecare mapper. La final, seturile pentru fiecare mapper le-am
stocat intr-un vector final, unde se pune lock la adaugarea fiecarui vector de
seturi ale fiecarui thread mapper.

Functia reduce - Se itereaza in vectorul cu seturile tuturor mapperilor si se
aleg puterile corespunzatoare fiecarui exponent.

Se pune bariera dupa ce se termina thread-urile mapper, pentru a nu incepe
cele reducer inainte ca acestea sa se termine.
