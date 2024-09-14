## Flat Hierarchy

```
a/b/c/c1.txt
a/b/c/c2.txt
a/b/d/d1.txt
a/b/d/d2.txt
a/e/f/f1.txt
a/e/f/f2.txt
a/e/g/g1.txt
a/e/g/g2.txt
```
     
### obj.checkm

```
a/b/c/c1.txt
a/b/c/c2.txt
a/b/d/d1.txt
a/b/d/d2.txt
a/e/f/f1.txt
a/e/f/f2.txt
a/e/g/g1.txt
a/e/g/g2.txt
```

### batch.1.checkm

b
```
c/c1.txt
c/c2.txt
d/d1.txt
d/d2.txt
```

e
```
f/f1.txt
f/f2.txt
g/g1.txt
g/g2.txt
```

### batch.2.checkm

c
```
c1.txt
c2.txt
```

d
```
d1.txt
d2.txt
```

f
```
f1.txt
f2.txt
```

g
```
g1.txt
g2.txt
```

## Extra Descendents - one node

```
a/b/c/c1.txt
a/b/c/c2.txt
a/b/d/d1.txt
a/b/d/d2.txt
a/e/f/f1.txt
a/e/f/f2.txt
a/e/g/h/h1.txt
a/e/g/h/h2.txt
```

### batch.1.checkm

b
```
c/c1.txt
c/c2.txt
d/d1.txt
d/d2.txt
```

e
```
f/f1.txt
f/f2.txt
g/h/h1.txt
g/h/h2.txt
```

### batch.2.checkm

c
```
c1.txt
c2.txt
```

d
```
d1.txt
d2.txt
```

f
```
f1.txt
f2.txt
```

g
```
h/h1.txt
h/h2.txt
```

### batch-1.checkm

c
```
c1.txt
c2.txt
```

d
```
d1.txt
d2.txt
```

f
```
f1.txt
f2.txt
```

h
```
h1.txt
h2.txt
```
### batch-2.checkm

b
```
c/c1.txt
c/c2.txt
d/d1.txt
d/d2.txt
```

e
```
f/f1.txt
f/f2.txt
g/h/h1.txt
g/h/h2.txt
```

## Extra Descendents - one half

```
a/b/c/c1.txt
a/b/c/c2.txt
a/b/d/d1.txt
a/b/d/d2.txt
a/e/f/i/i1.txt
a/e/f/i/i2.txt
a/e/g/h/h1.txt
a/e/g/h/h2.txt
```

### batch.1.checkm

b
```
c/c1.txt
c/c2.txt
d/d1.txt
d/d2.txt
```

e
```
f/i/i1.txt
f/i/i2.txt
g/h/h1.txt
g/h/h2.txt
```

### batch.2.checkm

c
```
c1.txt
c2.txt
```

d
```
d1.txt
d2.txt
```

f
```
i/i1.txt
i/i2.txt
```

g
```
h/h1.txt
h/h2.txt
```

### batch-1.checkm

c
```
c1.txt
c2.txt
```

d
```
d1.txt
d2.txt
```

i
```
i1.txt
i2.txt
```

h
```
h1.txt
h2.txt
```
### batch-2.checkm

b
```
c/c1.txt
c/c2.txt
d/d1.txt
d/d2.txt
```

f
```
i/i1.txt
i/i2.txt
```

g
```
h/h1.txt
h/h2.txt
```

