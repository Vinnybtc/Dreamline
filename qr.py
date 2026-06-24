# -*- coding: utf-8 -*-
"""Compacte, afhankelijkheidsvrije QR-generator (byte-mode, ECC niveau M, versie 1-10).
Genoeg voor een URL als http://192.168.1.23:8080. Levert een SVG."""

# --- capaciteit (byte-mode, niveau M) ---
CAP_M = {1:14,2:26,3:42,4:62,5:84,6:106,7:122,8:152,9:180,10:213}
# (ec_per_block, [(blokken, data_codewords_per_blok), ...])  niveau M
EC_M = {
 1:(10,[(1,16)]), 2:(16,[(1,28)]), 3:(26,[(1,44)]), 4:(18,[(2,32)]),
 5:(24,[(2,43)]), 6:(16,[(4,27)]), 7:(18,[(4,31)]),
 8:(22,[(2,38),(2,39)]), 9:(22,[(3,36),(2,37)]), 10:(26,[(4,43),(1,44)]),
}
ALIGN = {1:[],2:[6,18],3:[6,22],4:[6,26],5:[6,30],6:[6,34],
         7:[6,22,38],8:[6,24,42],9:[6,26,46],10:[6,28,50]}

# --- GF(256) ---
EXP=[0]*512; LOG=[0]*256
_x=1
for _i in range(255):
    EXP[_i]=_x; LOG[_x]=_i; _x<<=1
    if _x&0x100: _x^=0x11d
for _i in range(255,512): EXP[_i]=EXP[_i-255]
def gmul(a,b): return 0 if a==0 or b==0 else EXP[LOG[a]+LOG[b]]

def rs_gen(n):
    g=[1]
    for i in range(n):
        ng=[0]*(len(g)+1)
        for j in range(len(g)):
            ng[j]^=g[j]
            ng[j+1]^=gmul(g[j],EXP[i])
        g=ng
    return g

def rs_ec(data, n):
    g=rs_gen(n); res=[0]*n
    for d in data:
        f=d^res[0]; res=res[1:]+[0]
        if f:
            for i in range(n): res[i]^=gmul(g[i+1],f)
    return res

def bch15(data5):
    d=data5<<10
    for i in range(14,9,-1):
        if (d>>i)&1: d^=0x537<<(i-10)
    return ((data5<<10)|d)^0x5412

def bch18(v):
    d=v<<12
    for i in range(17,11,-1):
        if (d>>i)&1: d^=0x1f25<<(i-12)
    return (v<<12)|d

def _pick_version(n):
    for v in range(1,11):
        if CAP_M[v]>=n: return v
    raise ValueError("URL te lang voor QR v1-10")

def encode(text):
    data=text.encode("utf-8"); n=len(data)
    ver=_pick_version(n); size=17+4*ver
    ecpb,groups=EC_M[ver]
    total_data=sum(b*c for b,c in groups)

    # bitstroom
    bits=[]
    def put(val,length):
        for i in range(length-1,-1,-1): bits.append((val>>i)&1)
    put(0b0100,4)                       # byte-mode
    put(n, 8 if ver<10 else 16)         # char count
    for b in data: put(b,8)
    put(0, min(4, total_data*8-len(bits)))   # terminator
    while len(bits)%8: bits.append(0)
    pad=[0xEC,0x11]; k=0
    while len(bits)//8 < total_data:
        put(pad[k%2],8); k+=1
    codewords=[int("".join(map(str,bits[i:i+8])),2) for i in range(0,len(bits),8)]

    # blokken + ec
    blocks=[]; idx=0
    for cnt,dcw in groups:
        for _ in range(cnt):
            blk=codewords[idx:idx+dcw]; idx+=dcw
            blocks.append((blk, rs_ec(blk,ecpb)))
    # interleave
    final=[]
    maxd=max(len(b[0]) for b in blocks)
    for i in range(maxd):
        for blk,_ec in blocks:
            if i<len(blk): final.append(blk[i])
    for i in range(ecpb):
        for _blk,ec in blocks:
            final.append(ec[i])
    bitstream=[]
    for cw in final:
        for i in range(7,-1,-1): bitstream.append((cw>>i)&1)

    # matrix
    m=[[None]*size for _ in range(size)]
    res=[[False]*size for _ in range(size)]
    def setf(r,c,val): m[r][c]=val; res[r][c]=True
    def finder(r,c):
        for dr in range(-1,8):
            for dc in range(-1,8):
                rr,cc=r+dr,c+dc
                if 0<=rr<size and 0<=cc<size:
                    on = (0<=dr<=6 and 0<=dc<=6 and (dr in(0,6) or dc in(0,6) or (2<=dr<=4 and 2<=dc<=4)))
                    setf(rr,cc,1 if on else 0)
    finder(0,0); finder(0,size-7); finder(size-7,0)
    for i in range(8):                  # separators al gedekt door finder-rand
        pass
    # timing
    for i in range(8,size-8):
        v=1 if i%2==0 else 0
        if m[6][i] is None: setf(6,i,v)
        if m[i][6] is None: setf(i,6,v)
    # alignment
    ac=ALIGN[ver]
    for r in ac:
        for c in ac:
            if (r<8 and c<8) or (r<8 and c>size-9) or (r>size-9 and c<8): continue
            for dr in range(-2,3):
                for dc in range(-2,3):
                    on = dr in(-2,2) or dc in(-2,2) or (dr==0 and dc==0)
                    setf(r+dr,c+dc,1 if on else 0)
    # dark module
    setf(size-8,8,1)
    # reserve format
    for c in (0,1,2,3,4,5,7,8): res[8][c]=True
    for r in (0,1,2,3,4,5,7):   res[r][8]=True
    for i in range(7):          res[size-7+i][8]=True   # copy2 verticaal: col8 rijen size-7..size-1
    for i in range(8):          res[8][size-8+i]=True    # copy2 horizontaal: rij8 kolommen size-8..size-1
    # reserve version info
    if ver>=7:
        for k in range(18):
            a=size-11+k%3; b=k//3
            res[a][b]=True; res[b][a]=True

    # data placement
    di=0; up=True; col=size-1
    while col>0:
        if col==6: col-=1
        for k in range(size):
            r=(size-1-k) if up else k
            for c in (col,col-1):
                if not res[r][c]:
                    bit=bitstream[di] if di<len(bitstream) else 0; di+=1
                    m[r][c]=bit
        up=not up; col-=2

    # masking
    def mask_fn(p,i,j):
        return [ (i+j)%2==0, i%2==0, j%3==0, (i+j)%3==0,
                 (i//2+j//3)%2==0, (i*j)%2+(i*j)%3==0,
                 ((i*j)%2+(i*j)%3)%2==0, ((i+j)%2+(i*j)%3)%2==0 ][p]
    def penalty(g):
        s=0
        for line in (g, [list(x) for x in zip(*g)]):
            for row in line:
                run=1
                for x in range(1,size):
                    if row[x]==row[x-1]: run+=1
                    else:
                        if run>=5: s+=3+(run-5)
                        run=1
                if run>=5: s+=3+(run-5)
        for r in range(size-1):
            for c in range(size-1):
                if g[r][c]==g[r][c+1]==g[r+1][c]==g[r+1][c+1]: s+=3
        dark=sum(sum(row) for row in g); tot=size*size
        ratio=dark*100//tot; s+=10*(min(abs(ratio-50)//5, 100))
        return s

    best=None; bestg=None; bestmask=0
    for mask in range(8):
        g=[[ (m[r][c]^(1 if mask_fn(mask,r,c) else 0)) if not _is_func(res,r,c,m) else m[r][c]
             for c in range(size)] for r in range(size)]
        # format info voor deze mask
        _place_format(g,size,bch15((0b00<<3)|mask))
        if ver>=7: _place_version(g,size,bch18(ver))
        p=penalty(g)
        if best is None or p<best: best=p; bestg=g; bestmask=mask
    return bestg

def _is_func(res,r,c,m):
    # functiemodule = gereserveerd (res True) — die niet maskeren
    return res[r][c]

def _place_format(g,size,fmt):
    gb=lambda i:(fmt>>i)&1
    for i in range(15):                 # verticaal (kolom 8)
        b=gb(i)
        if i<6:   g[i][8]=b
        elif i<8: g[i+1][8]=b
        else:     g[size-15+i][8]=b
    for i in range(15):                 # horizontaal (rij 8)
        b=gb(i)
        if i<8:   g[8][size-1-i]=b
        elif i==8:g[8][7]=b
        else:     g[8][14-i]=b
    g[size-8][8]=1                      # vaste donkere module

def _place_version(g,size,ver18):
    for k in range(18):
        bit=(ver18>>k)&1
        a=size-11+k%3; b=k//3
        g[a][b]=bit; g[b][a]=bit

def svg(text, scale=8, border=4, dark="#15110e", light="#ffffff"):
    g=encode(text); size=len(g); dim=(size+2*border)*scale
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{dim}" height="{dim}" '
           f'viewBox="0 0 {dim} {dim}" shape-rendering="crispEdges">',
           f'<rect width="{dim}" height="{dim}" fill="{light}"/>']
    for r in range(size):
        for c in range(size):
            if g[r][c]:
                x=(c+border)*scale; y=(r+border)*scale
                parts.append(f'<rect x="{x}" y="{y}" width="{scale}" height="{scale}" fill="{dark}"/>')
    parts.append("</svg>")
    return "".join(parts)

if __name__=="__main__":
    import sys
    print(svg(sys.argv[1] if len(sys.argv)>1 else "http://192.168.1.23:8080"))
