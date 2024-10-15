/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <sys/types.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>

static inline char	*med3 (char *, char *, char *,
    int (*)(const void *, const void *));
static inline void	 swapfunc (char *, char *, size_t, int);

#define min(a, b)	(a) < (b) ? a : b

/*
 * Qsort routine from Bentley & McIlroy's "Engineering a Sort Function".
 */
#define swapcode(TYPE, parmi, parmj, n) { 		\
	size_t i = (n) / sizeof (TYPE); 		\
	TYPE *pi = (TYPE *)(void *)(parmi); 		\
	TYPE *pj = (TYPE *)(void *)(parmj); 		\
	do { 						\
		TYPE	t = *pi;			\
		*pi++ = *pj;				\
		*pj++ = t;				\
        } while (--i > 0);				\
}

#define SWAPINIT(a, es) swaptype = (uintptr_t)a % sizeof(long) || \
	es % sizeof(long) ? 2 : es == sizeof(long)? 0 : 1;

static inline void
swapfunc(char *a, char *b, size_t n, int swaptype)
{

	if (swaptype <= 1)
		swapcode(long, a, b, n)
	else
		swapcode(char, a, b, n)
}

#define swap(a, b)						\
	if (swaptype == 0) {					\
		long t = *(long *)(void *)(a);			\
		*(long *)(void *)(a) = *(long *)(void *)(b);	\
		*(long *)(void *)(b) = t;			\
	} else							\
		swapfunc(a, b, es, swaptype)

#define vecswap(a, b, n) if ((n) > 0) swapfunc((a), (b), (size_t)(n), swaptype)

static inline char *
med3(char *a, char *b, char *c,
    int (*cmp) (const void *, const void *))
{

	return cmp(a, b) < 0 ?
	       (cmp(b, c) < 0 ? b : (cmp(a, c) < 0 ? c : a ))
              :(cmp(b, c) > 0 ? b : (cmp(a, c) < 0 ? a : c ));
}

static void
_pqsort(void *a, size_t n, size_t es,
    int (*cmp) (const void *, const void *), void *lrange, void *rrange)
{
	char *pa, *pb, *pc, *pd, *pl, *pm, *pn;
	size_t d, r;
	int swaptype, cmp_result;

loop:	SWAPINIT(a, es);
	if (n < 7) {
		for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl) > 0;
			     pl -= es)
				swap(pl, pl - es);
		return;
	}
	pm = (char *) a + (n / 2) * es;
	if (n > 7) {
		pl = (char *) a;
		pn = (char *) a + (n - 1) * es;
		if (n > 40) {
			d = (n / 8) * es;
			pl = med3(pl, pl + d, pl + 2 * d, cmp);
			pm = med3(pm - d, pm, pm + d, cmp);
			pn = med3(pn - 2 * d, pn - d, pn, cmp);
		}
		pm = med3(pl, pm, pn, cmp);
	}
	swap(a, pm);
	pa = pb = (char *) a + es;

	pc = pd = (char *) a + (n - 1) * es;
	for (;;) {
		while (pb <= pc && (cmp_result = cmp(pb, a)) <= 0) {
			if (cmp_result == 0) {
				swap(pa, pb);
				pa += es;
			}
			pb += es;
		}
		while (pb <= pc && (cmp_result = cmp(pc, a)) >= 0) {
			if (cmp_result == 0) {
				swap(pc, pd);
				pd -= es;
			}
			pc -= es;
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		pb += es;
		pc -= es;
	}

	pn = (char *) a + n * es;
	r = min(pa - (char *) a, pb - pa);
	vecswap(a, pb - r, r);
	r = min((size_t)(pd - pc), pn - pd - es);
	vecswap(pb, pn - r, r);
	if ((r = pb - pa) > es) {
                void *_l = a, *_r = ((unsigned char*)a)+r-1;
                if (!((lrange < _l && rrange < _l) ||
                    (lrange > _r && rrange > _r)))
		    _pqsort(a, r / es, es, cmp, lrange, rrange);
        }
	if ((r = pd - pc) > es) {
                void *_l, *_r;

		/* Iterate rather than recurse to save stack space */
		a = pn - r;
		n = r / es;

                _l = a;
                _r = ((unsigned char*)a)+r-1;
                if (!((lrange < _l && rrange < _l) ||
                    (lrange > _r && rrange > _r)))
		    goto loop;
	}
/*		qsort(pn - r, r / es, es, cmp);*/
}

void
pqsort(void *a, size_t n, size_t es,
    int (*cmp) (const void *, const void *), size_t lrange, size_t rrange)
{
    _pqsort(a,n,es,cmp,((unsigned char*)a)+(lrange*es),
                       ((unsigned char*)a)+((rrange+1)*es)-1);
}
