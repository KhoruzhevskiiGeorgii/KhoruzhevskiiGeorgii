from __future__ import annotations
import csv, hashlib, json, math, os, statistics, time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT=Path('output'); CACHE=OUT/'raw'/'cache'; CACHE.mkdir(parents=True,exist_ok=True)
PERIODS=('WEEK','MONTH'); CATEGORIES=('OVERALL','POLITICS','ECONOMICS','FINANCE','TECH','CRYPTO')
NOW=datetime.now(timezone.utc); CONCURRENCY=16; ACTIVITY_LIMIT=500; CLOSED_LIMIT=500; PRICE_SAMPLES=12
DATA='https://data-api.polymarket.com'; CLOB='https://clob.polymarket.com'
RULES={'days_since_last_trade':('max',3.0),'active_days_30d':('min',8),'closed_positions':('min',50),'visible_history_days':('min',90.0),'realized_roi':('min',.15),'trades_per_active_day':('max',10.0),'trades_per_market':('max',3.0),'top1_positive_pnl_share':('max',.20),'top5_positive_pnl_share':('max',.50),'median_holding_days':('min',1.0),'hedged_market_share':('max',.05),'median_adverse_1m':('max',.02)}

def cache_path(url): return CACHE/(hashlib.sha256(url.encode()).hexdigest()+'.json')
def get_json(base,path,params,retries=4):
    url=base+path+'?'+urlencode({k:v for k,v in params.items() if v is not None},doseq=True); cp=cache_path(url)
    if cp.exists(): return json.loads(cp.read_text())['payload']
    err=None
    for i in range(retries+1):
        try:
            req=Request(url,headers={'User-Agent':'polymarket-copytrade-scan/0.1'})
            with urlopen(req,timeout=25) as r: payload=json.loads(r.read().decode())
            cp.write_text(json.dumps({'url':url,'retrieved_at':NOW.isoformat(),'payload':payload},ensure_ascii=False))
            return payload
        except Exception as e:
            err=e
            if i<retries: time.sleep(min(.25*(2**i),2))
    raise RuntimeError(f'{url}: {err}')

def leaderboard(cat,period): return get_json(DATA,'/v1/leaderboard',{'category':cat,'timePeriod':period,'orderBy':'PNL','limit':50,'offset':0})
def activity(wallet): return get_json(DATA,'/activity',{'user':wallet,'type':'TRADE','limit':ACTIVITY_LIMIT,'offset':0,'sortBy':'TIMESTAMP','sortDirection':'DESC'})
def positions(wallet): return get_json(DATA,'/positions',{'user':wallet,'sizeThreshold':0,'limit':500,'offset':0})
def closed(wallet):
    out=[]
    for off in range(0,CLOSED_LIMIT,50):
        page=get_json(DATA,'/closed-positions',{'user':wallet,'limit':50,'offset':off,'sortBy':'TIMESTAMP','sortDirection':'DESC'})
        out.extend(page)
        if len(page)<50: break
    return out[:CLOSED_LIMIT]
def price_history(asset,start,end): return get_json(CLOB,'/prices-history',{'market':asset,'startTs':start,'endTs':end,'interval':'1m','fidelity':1}).get('history',[])
def num(x):
    try: return float(x)
    except: return None
def ts(x):
    n=num(x)
    if not n or n<=0:return None
    if n>1e10:n/=1000
    return int(n)
def dt(x):
    t=ts(x)
    return datetime.fromtimestamp(t,tz=timezone.utc) if t else None
def cond(r): return str(r.get('conditionId') or r.get('market') or r.get('slug') or '')
def med(xs): return statistics.median(xs) if xs else None
def safe_div(a,b): return a/b if a is not None and b not in (None,0) else None

def collect_leaderboards():
    rows=[]; errors=[]
    for cat in CATEGORIES:
        for period in PERIODS:
            try:
                for r in leaderboard(cat,period): rows.append({**r,'category':cat,'timePeriod':period,'retrievedAt':NOW.isoformat(),'sourceUrl':f'{DATA}/v1/leaderboard?category={cat}&timePeriod={period}&orderBy=PNL&limit=50&offset=0'})
            except Exception as e: errors.append({'wallet':'','endpoint':f'leaderboard:{cat}:{period}','error':repr(e)})
    return rows,errors

def dedupe(rows):
    d={}
    for r in rows:
        w=str(r.get('proxyWallet') or '').lower()
        if not w:continue
        c=d.setdefault(w,{'wallet':w,'username':r.get('userName'),'x_username':r.get('xUsername'),'memberships':[],'leaderboard_best_rank':None,'leaderboard_max_pnl':None,'leaderboard_max_volume':None})
        m=f"{r.get('category')}:{r.get('timePeriod')}"
        if m not in c['memberships']:c['memberships'].append(m)
        try: rank=int(r.get('rank'))
        except: rank=None
        if rank is not None:c['leaderboard_best_rank']=rank if c['leaderboard_best_rank'] is None else min(c['leaderboard_best_rank'],rank)
        for sk,tk in [('pnl','leaderboard_max_pnl'),('vol','leaderboard_max_volume')]:
            v=num(r.get(sk))
            if v is not None:c[tk]=v if c[tk] is None else max(c[tk],v)
    for c in d.values():c['memberships']=sorted(c['memberships'])
    return list(d.values())

def metrics(c,acts,cls,pos):
    trades=[r for r in acts if str(r.get('type') or 'TRADE').upper()=='TRADE']; times=[dt(r.get('timestamp')) for r in trades]; times=[x for x in times if x]
    last=max(times) if times else None; first=min(times) if times else None; dates={x.date() for x in times}; dates30={x.date() for x in times if (NOW-x).total_seconds()<=30*86400}
    conditions={cond(r) for r in trades if cond(r)}; limitations=[]
    if not trades:limitations.append('No activity records returned')
    if len(acts)>=ACTIVITY_LIMIT:limitations.append(f'Activity truncated at {ACTIVITY_LIMIT} rows')
    pnl=[]; costs=[]; pos_by=defaultdict(float); close_at={}
    for r in cls:
        p=num(r.get('realizedPnl'))
        if p is not None:pnl.append(p);pos_by[cond(r)]+=max(p,0)
        tb=num(r.get('totalBought'));ap=num(r.get('avgPrice'))
        if tb is not None and ap is not None:costs.append(tb*ap)
        d=dt(r.get('timestamp'));k=cond(r)
        if d and k:close_at[k]=max(close_at.get(k,d),d)
    rp=sum(pnl) if pnl else None; cost=sum(costs) if costs else None; roi=safe_div(rp,cost)
    positives=sorted([v for v in pos_by.values() if v>0],reverse=True); pt=sum(positives)
    first_buy={}; outcomes=defaultdict(set)
    for r in trades:
        if str(r.get('side') or '').upper()!='BUY':continue
        k=cond(r); d=dt(r.get('timestamp'))
        if k and d:first_buy[k]=min(first_buy.get(k,d),d)
        marker=r.get('outcomeIndex',r.get('asset') or r.get('outcome'))
        if k and marker is not None:outcomes[k].add(str(marker))
    holds=[(d-first_buy[k]).total_seconds()/86400 for k,d in close_at.items() if k in first_buy and d>=first_buy[k]]
    hedged=sum(1 for s in outcomes.values() if len(s)>1); categories=[m.split(':',1)[0] for m in c['memberships']]; non=[x for x in categories if x!='OVERALL']
    tpad=safe_div(len(trades),len(dates)); tpm=safe_div(len(trades),len(conditions)); truncated=len(acts)>=ACTIVITY_LIMIT
    row={
      'wallet':c['wallet'],'username':c.get('username'),'x_username':c.get('x_username'),'leaderboard_memberships':', '.join(c['memberships']),'primary_category':non[0] if non else (categories[0] if categories else None),'is_crypto_candidate':'CRYPTO' in categories,
      'last_trade_at':last.isoformat() if last else None,'days_since_last_trade':(NOW-last).total_seconds()/86400 if last else None,'active_days_30d':len(dates30),'visible_history_days':(last-first).total_seconds()/86400 if last and first else None,'trade_count':len(trades),'unique_markets':len(conditions),'trades_per_active_day':tpad,'trades_per_market':tpm,'activity_truncated':truncated,
      'closed_positions':len(cls),'realized_pnl':rp,'realized_cost_basis':cost,'realized_roi':roi,'top1_positive_pnl_share':safe_div(positives[0],pt) if positives else None,'top5_positive_pnl_share':safe_div(sum(positives[:5]),pt) if positives else None,'median_holding_days':med(holds),'holding_sample_size':len(holds),
      'hedged_markets':hedged,'hedged_market_share':safe_div(hedged,len(outcomes)),'hft_flag':bool(truncated or (tpad is not None and tpad>10) or (tpm is not None and tpm>10)),'median_adverse_1m':None,'median_adverse_5m':None,'persistence_sample_size':0,
      'current_positions':len(pos),'current_value':sum(num(r.get('currentValue')) or 0 for r in pos) if pos else None,'current_cash_pnl':sum(num(r.get('cashPnl')) or 0 for r in pos) if pos else None,'copyability_score':None,'failed_rule_count':None,'decision':None,'rejection_reasons':'','data_limitations':'; '.join(limitations),'source_profile_url':f"https://polymarket.com/profile/{c['wallet']}"}
    return row

def inv(v,good,bad): return 0 if v is None else max(0,min(1,(bad-v)/(bad-good)))
def lin(v,low,high): return 0 if v is None else max(0,min(1,(v-low)/(high-low)))
def score(c):
    s=10*inv(c['days_since_last_trade'],0,10)+10*lin(c['active_days_30d'],4,15)+10*lin(c['closed_positions'],10,100)+10*lin(c['visible_history_days'],30,180)+20*lin(c['realized_roi'],0,.3)
    s+=15*(.5*inv(c['top1_positive_pnl_share'],.1,.6)+.5*inv(c['top5_positive_pnl_share'],.3,.95));s+=10*(.5*inv(c['trades_per_active_day'],2,30)+.5*inv(c['trades_per_market'],1,10));s+=5*lin(c['median_holding_days'],0,7)+5*inv(c['hedged_market_share'],0,.3)+5*inv(c['median_adverse_1m'],0,.1)
    if c['hft_flag']:s-=10
    if c['is_crypto_candidate']:s-=5
    return round(max(0,min(100,s)),2)
def decide(c):
    reasons=[]
    labels={'days_since_last_trade':'Days since last trade','active_days_30d':'Active days (30d)','closed_positions':'Closed positions','visible_history_days':'Visible history days','realized_roi':'ROI','trades_per_active_day':'Trades per active day','trades_per_market':'Trades per market','top1_positive_pnl_share':'Top-1 positive PnL share','top5_positive_pnl_share':'Top-5 positive PnL share','median_holding_days':'Median holding days','hedged_market_share':'Hedged market share','median_adverse_1m':'Median adverse move 1m'}
    for k,(op,limit) in RULES.items():
        v=c.get(k)
        if v is None:reasons.append(labels[k]+' unavailable')
        elif op=='min' and v<limit:reasons.append(f'{labels[k]} {v:.3g} < {limit:.3g}')
        elif op=='max' and v>limit:reasons.append(f'{labels[k]} {v:.3g} > {limit:.3g}')
    if c['hft_flag'] and not any('Trades per active day' in x for x in reasons):reasons.append('HFT/activity truncation flag')
    c['copyability_score']=score(c);c['failed_rule_count']=len(reasons);c['decision']='PASS' if not reasons else 'REJECT';c['rejection_reasons']='; '.join(reasons)

def price_after(hist,target,tol=180):
    pts=[(ts(x.get('t')),num(x.get('p'))) for x in hist];pts=[x for x in pts if x[0] and x[1] is not None and target<=x[0]<=target+tol]
    return min(pts)[1] if pts else None
def persistence(wallet,acts):
    first={}
    for r in sorted(acts,key=lambda x:ts(x.get('timestamp')) or 0):
        if str(r.get('side') or '').upper()!='BUY':continue
        k=cond(r);a=str(r.get('asset') or '');t=ts(r.get('timestamp'));p=num(r.get('price'))
        if k and a and t and p is not None:first.setdefault(k,r)
    rows=[];a1=[];a5=[]
    for r in sorted(first.values(),key=lambda x:ts(x.get('timestamp')) or 0,reverse=True)[:PRICE_SAMPLES]:
        t=ts(r.get('timestamp'));p=num(r.get('price'));a=str(r.get('asset'));err=None
        try:h=price_history(a,t-60,t+600)
        except Exception as e:h=[];err=repr(e)
        p1=price_after(h,t+60);p5=price_after(h,t+300);d1=p1-p if p1 is not None else None;d5=p5-p if p5 is not None else None
        if d1 is not None:a1.append(d1)
        if d5 is not None:a5.append(d5)
        rows.append({'wallet':wallet,'timestamp':t,'condition_id':r.get('conditionId'),'asset':a,'title':r.get('title'),'outcome':r.get('outcome'),'entry_price':p,'price_1m':p1,'price_5m':p5,'adverse_1m':d1,'adverse_5m':d5,'error':err,'source_url':f'{CLOB}/prices-history?market={a}&startTs={t-60}&endTs={t+600}&interval=1m&fidelity=1'})
    return rows,med(a1),med(a5),max(len(a1),len(a5))
def fetch(fn,w):
    try:return w,fn(w),None
    except Exception as e:return w,[],repr(e)
def write_csv(name,rows):
    rows=list(rows);p=OUT/name
    if not rows:p.write_text('');return
    fields=[]
    for r in rows:
        for k in r:
            if k not in fields:fields.append(k)
    with p.open('w',newline='',encoding='utf-8-sig') as f:
        wr=csv.DictWriter(f,fieldnames=fields);wr.writeheader();wr.writerows(rows)
def write_json(name,obj): (OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str))

def main():
    OUT.mkdir(exist_ok=True); lb,errors=collect_leaderboards(); cands=dedupe(lb); acts={}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        fut={ex.submit(fetch,activity,c['wallet']):c['wallet'] for c in cands}
        for f in as_completed(fut):w,r,e=f.result();acts[w]=r;errors.extend([{'wallet':w,'endpoint':'activity','error':e}] if e else [])
    prelim={c['wallet']:metrics(c,acts.get(c['wallet'],[]),[],[]) for c in cands}; detailed={w for w,m in prelim.items() if m['days_since_last_trade'] is not None and m['days_since_last_trade']<=14 and m['active_days_30d']>=2};detailed|={c['wallet'] for c in sorted(cands,key=lambda x:-(x.get('leaderboard_max_pnl') or 0))[:20]}
    cls={w:[] for w in prelim};pos={w:[] for w in prelim}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        fut={}
        for w in detailed:fut[ex.submit(fetch,closed,w)]=(w,'closed_positions');fut[ex.submit(fetch,positions,w)]=(w,'positions')
        for f in as_completed(fut):w,ep=fut[f];_,r,e=f.result();(cls if ep=='closed_positions' else pos)[w]=r;errors.extend([{'wallet':w,'endpoint':ep,'error':e}] if e else [])
    by={c['wallet']:c for c in cands};rows=[]
    for w,c in by.items():
        m=metrics(c,acts.get(w,[]),cls.get(w,[]),pos.get(w,[]));m['copyability_score']=score(m)
        if w not in detailed:m['data_limitations']=(m['data_limitations']+'; ' if m['data_limitations'] else '')+'Detailed closed-position fetch skipped after inactivity prefilter'
        rows.append(m)
    targets=sorted([r for r in rows if r['days_since_last_trade'] is not None and r['days_since_last_trade']<=14 and r['active_days_30d']>=2 and r['closed_positions']>0],key=lambda x:-(x['copyability_score'] or 0))[:30];pers=[]
    for r in targets:
        rr,m1,m5,n=persistence(r['wallet'],acts.get(r['wallet'],[]));pers+=rr;r['median_adverse_1m']=m1;r['median_adverse_5m']=m5;r['persistence_sample_size']=n
    for r in rows:decide(r)
    rows.sort(key=lambda x:(x['decision']!='PASS',x['failed_rule_count'],-(x['copyability_score'] or 0)))
    categories={}
    for r in rows:
        k=r['primary_category'] or 'UNKNOWN';b=categories.setdefault(k,{'category':k,'candidates':0,'passed':0,'crypto_candidates':0,'scores':[]});b['candidates']+=1;b['passed']+=r['decision']=='PASS';b['crypto_candidates']+=r['is_crypto_candidate'];b['scores'].append(r['copyability_score'])
    catrows=[]
    for b in categories.values():s=b.pop('scores');b['avg_score']=sum(s)/len(s) if s else None;catrows.append(b)
    rawacts=[{'wallet':w,**r} for w,x in acts.items() for r in x];rawcls=[{'wallet':w,**r} for w,x in cls.items() for r in x];rawpos=[{'wallet':w,**r} for w,x in pos.items() for r in x]
    near=sorted([r for r in rows if r['decision']!='PASS'],key=lambda x:(x['failed_rule_count'],-(x['copyability_score'] or 0)))[:25]
    write_csv('candidates.csv',rows);write_csv('passed.csv',[r for r in rows if r['decision']=='PASS']);write_csv('near_misses.csv',near);write_csv('rejected.csv',[r for r in rows if r['decision']=='REJECT']);write_csv('price_persistence.csv',pers);write_csv('category_mix.csv',catrows);write_csv('errors.csv',errors)
    write_json('raw_leaderboards.json',lb);write_json('raw_activity.json',rawacts);write_json('raw_closed_positions.json',rawcls);write_json('raw_current_positions.json',rawpos);write_json('scan_summary.json',{'generated_at':NOW.isoformat(),'leaderboard_rows':len(lb),'candidate_count':len(rows),'detailed_wallets':len(detailed),'passed_count':sum(r['decision']=='PASS' for r in rows),'persistence_rows':len(pers),'error_count':len(errors)})
    print(json.dumps(json.loads((OUT/'scan_summary.json').read_text()),indent=2))
if __name__=='__main__':main()
