const express=require('express');const http=require('http');const crypto=require('crypto');const {Server}=require('socket.io');const QRCode=require('qrcode');
const {Pool}=require('pg');
const app=express(),server=http.createServer(app),io=new Server(server);app.set('trust proxy',1);app.use(express.json({limit:'32kb'}));app.use(express.static('public',{maxAge:'1h'}));app.get('/health',async(_q,r)=>r.json({ok:true,service:'family-feud-realtime',version:'6.0.0',database:!!process.env.DATABASE_URL}));
app.get('/qr/:code',async(req,res)=>{try{const c=clean(req.params.code);if(!/^[A-Z0-9]{4,12}$/.test(c))return res.status(400).end();const url=`${req.protocol}://${req.get('host')}/?join=${encodeURIComponent(c)}`;const png=await QRCode.toBuffer(url,{width:640,margin:2,errorCorrectionLevel:'M'});res.type('png').send(png)}catch(e){res.status(500).end()}});
const games=new Map();let pool=null;
if(process.env.DATABASE_URL){pool=new Pool({connectionString:process.env.DATABASE_URL,ssl:process.env.DATABASE_URL.includes('localhost')?false:{rejectUnauthorized:false}});}
const clean=v=>String(v||'').trim().toUpperCase();const hash=v=>crypto.createHash('sha256').update(String(v)).digest('hex');
function code(){let c;do c=crypto.randomBytes(3).toString('hex').toUpperCase();while(games.has(c));return c}function pin(){return String(Math.floor(100000+Math.random()*900000));}
function fresh(){return {code:code(),host:null,hostPinHash:null,displays:new Set(),teams:[{id:'A',name:'الفريق الأحمر',score:0,strikes:0,players:[]},{id:'B',name:'الفريق الأزرق',score:0,strikes:0,players:[]}],rounds:5,time:30,currentRound:0,question:'',answers:[],phase:'lobby',timer:30,lock:null,questionBank:defaultQuestions()};}
function defaultQuestions(){return [
{q:'اذكر شيئًا يأخذه الناس معهم إلى البحر',a:[['منشفة',35],['ماء',25],['نظارة شمسية',18],['واقي شمس',12],['ملابس سباحة',7],['كرة',3]]},
{q:'اذكر شيئًا تجده عادةً في المطبخ',a:[['ثلاجة',35],['فرن',25],['صحون',18],['ملعقة',12],['سكين',7],['كوب',3]]},
{q:'اذكر شيئًا يفعله الناس قبل النوم',a:[['تصفح الجوال',30],['تنظيف الأسنان',25],['مشاهدة التلفزيون',18],['قراءة',12],['شرب ماء',8],['إطفاء الأنوار',7]]},
{q:'اذكر شيئًا ينساه الناس عندما يخرجون من البيت',a:[['المفاتيح',35],['الجوال',25],['المحفظة',18],['الشاحن',10],['المظلة',7],['النظارة',5]]},
{q:'اذكر شيئًا مشهورًا في حفلات الزواج',a:[['الكيكة',30],['الرقص',25],['العشاء',18],['التصوير',12],['الزفة',10],['الهدايا',5]]}
];}
function sanitizeBank(bank){return (Array.isArray(bank)?bank:[]).slice(0,100).map(x=>({q:String(x.q||'').trim().slice(0,200),a:(Array.isArray(x.a)?x.a:[]).slice(0,8).map(v=>[String(v[0]||'').trim().slice(0,80),Math.max(0,Math.min(1000,+v[1]||0))]).filter(v=>v[0])})).filter(x=>x.q&&x.a.length);}
function pub(g,role){
  const base={code:g.code,teams:g.teams.map(t=>({id:t.id,name:t.name,score:t.score,strikes:t.strikes,players:t.players.map(p=>({name:p.name}))})),rounds:g.rounds,time:g.time,currentRound:g.currentRound,question:g.question,phase:g.phase,timer:g.timer,lock:g.lock};
  if(role==='host'){
    return {...base,answers:g.answers,questionBank:g.questionBank||[]};
  }
  return {...base,answers:(g.answers||[]).map(a=>a.revealed?{text:a.text,points:a.points,revealed:true}:{revealed:false})};
}
async function save(g){if(!pool)return;await pool.query(`INSERT INTO games(code,state,updated_at) VALUES($1,$2,NOW()) ON CONFLICT(code) DO UPDATE SET state=$2,updated_at=NOW()`,[g.code,JSON.stringify({...g,host:null,displays:[],teams:g.teams.map(t=>({...t,players:[]}))})]).catch(console.error)}
async function init(){if(!pool)return;await pool.query(`CREATE TABLE IF NOT EXISTS games(code TEXT PRIMARY KEY,state JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`);const rs=await pool.query(`SELECT code,state FROM games WHERE updated_at>NOW()-INTERVAL '24 hours'`);for(const r of rs.rows){const g=r.state;g.code=r.code;g.host=null;g.displays=new Set();g.teams.forEach(t=>t.players=[]);g.questionBank=sanitizeBank(g.questionBank||defaultQuestions());games.set(g.code,g)}}
function emit(g){
  const room=io.sockets.adapter.rooms.get(g.code)||new Set();
  for(const sid of room){
    const sock=io.sockets.sockets.get(sid);
    if(sock) sock.emit('state',pub(g,sock.data?.role));
  }
  save(g);
}function get(c){return games.get(clean(c))}
io.on('connection',s=>{
 s.on('host:create',async({rounds=5,time=30,teamA,teamB}={})=>{const g=fresh(),p=pin();g.host=s.id;g.hostPinHash=hash(p);g.rounds=Math.min(50,Math.max(1,+rounds||5));g.time=Math.min(600,Math.max(5,+time||30));g.timer=g.time;if(teamA)g.teams[0].name=String(teamA).trim().slice(0,30)||g.teams[0].name;if(teamB)g.teams[1].name=String(teamB).trim().slice(0,30)||g.teams[1].name;games.set(g.code,g);s.join(g.code);s.data={role:'host',game:g.code};await save(g);s.emit('game:created',{code:g.code,pin:p});emit(g)});
 s.on('host:rejoin',({code,pin}={})=>{const g=get(code);if(!g||!g.hostPinHash||hash(pin)!==g.hostPinHash)return s.emit('error:msg','رمز المقدم غير صحيح');g.host=s.id;s.join(g.code);s.data={role:'host',game:g.code};s.emit('host:rejoined');emit(g)});
 s.on('display:join',({code}={})=>{const g=get(code);if(!g)return s.emit('error:msg','رمز اللعبة غير صحيح');g.displays.add(s.id);s.join(g.code);s.data={role:'display',game:g.code};s.emit('display:joined');emit(g)});
 s.on('player:join',({code,name,team}={})=>{const g=get(code),t=g?.teams.find(x=>x.id===team),n=String(name||'').trim().slice(0,24);if(!g)return s.emit('error:msg','رمز اللعبة غير صحيح');if(!t)return s.emit('error:msg','اختر فريقًا');if(!n)return s.emit('error:msg','اكتب اسمك');g.teams.forEach(x=>x.players=x.players.filter(p=>p.socketId!==s.id));t.players.push({socketId:s.id,name:n});s.join(g.code);s.data={role:'player',game:g.code,team,name:n};s.emit('player:joined',{team,name:n});emit(g)});
 s.on('buzzer:press',({code}={})=>{const g=get(code);if(!g||s.data.game!==g.code||s.data.role!=='player'||g.phase!=='question'||g.lock)return;const t=g.teams.find(x=>x.id===s.data.team);if(!t||t.strikes>=3)return;g.lock={name:s.data.name,team:s.data.team};io.to(g.code).emit('buzzer:locked',g.lock);emit(g);setTimeout(()=>{const cur=games.get(g.code);if(cur&&cur.lock&&cur.lock.name===g.lock.name&&cur.lock.team===g.lock.team){cur.lock=null;emit(cur)}},3000)});
 s.on('host:startRound',({code,question,answers,questionIndex}={})=>{const g=get(code);if(!g||g.host!==s.id||g.currentRound>=g.rounds)return;let item=(Number.isInteger(questionIndex)&&g.questionBank[questionIndex])?g.questionBank[questionIndex]:null;if(item){question=item.q;answers=item.a.map(x=>({text:x[0],points:x[1]}));}g.currentRound++;g.question=String(question||'').slice(0,200);g.answers=(Array.isArray(answers)?answers:[]).slice(0,8).map(a=>({text:String(a.text||'').slice(0,80),points:Math.max(0,+a.points||0),revealed:false}));g.phase='question';g.timer=g.time;g.lock=null;emit(g)});
 s.on('host:saveBank',({code,bank}={})=>{const g=get(code);if(!g||g.host!==s.id)return;const cleanBank=sanitizeBank(bank);if(!cleanBank.length)return s.emit('error:msg','يجب أن تحتوي قاعدة الأسئلة على سؤال وإجابة واحدة على الأقل');g.questionBank=cleanBank;emit(g);s.emit('bank:saved');});
 s.on('host:addQuestion',({code,question}={})=>{const g=get(code);if(!g||g.host!==s.id)return;const b=sanitizeBank([...(g.questionBank||[]),question]);if(b.length>(g.questionBank||[]).length){g.questionBank=b;emit(g)}else s.emit('error:msg','السؤال أو الإجابات غير صالحة');});
 s.on('host:deleteQuestion',({code,index}={})=>{const g=get(code);if(!g||g.host!==s.id)return;g.questionBank=(g.questionBank||[]).filter((_,i)=>i!==index);if(!g.questionBank.length)g.questionBank=defaultQuestions();emit(g)});
 s.on('host:clearRound',({code}={})=>{const g=get(code);if(!g||g.host!==s.id)return;g.question='';g.answers=[];g.lock=null;g.timer=g.time;g.phase='lobby';emit(g)});
 s.on('host:reveal',({code,index}={})=>{const g=get(code);if(!g||g.host!==s.id||!g.answers[index])return;g.answers[index].revealed=true;emit(g)});
 s.on('host:award',({code,team,points}={})=>{const g=get(code);if(!g||g.host!==s.id)return;const t=g.teams.find(x=>x.id===team);if(t)t.score+=Math.max(0,+points||0);emit(g)});
 s.on('host:strike',({code,team}={})=>{const g=get(code);if(!g||g.host!==s.id)return;const t=g.teams.find(x=>x.id===team);if(t)t.strikes=Math.min(3,t.strikes+1);emit(g)});
 s.on('host:resetStrikes',({code,team}={})=>{const g=get(code);if(!g||g.host!==s.id)return;const t=g.teams.find(x=>x.id===team);if(t)t.strikes=0;emit(g)});
 s.on('host:resetScores',({code}={})=>{const g=get(code);if(!g||g.host!==s.id)return;g.teams.forEach(t=>t.score=0);emit(g)});
 s.on('host:next',({code}={})=>{const g=get(code);if(!g||g.host!==s.id)return;g.phase=g.currentRound>=g.rounds?'finished':'lobby';g.question='';g.answers=[];g.timer=g.time;g.lock=null;g.teams.forEach(t=>t.strikes=0);emit(g)});s.on('host:end',({code}={})=>{const g=get(code);if(!g||g.host!==s.id)return;g.phase='finished';g.lock=null;emit(g)});
 s.on('disconnect',()=>{const g=games.get(s.data?.game);if(!g)return;g.teams.forEach(t=>t.players=t.players.filter(p=>p.socketId!==s.id));g.displays.delete(s.id);if(g.host===s.id)g.host=null;emit(g)})
});
setInterval(()=>{for(const g of games.values())if(g.phase==='question'&&g.timer>0){g.timer--;io.to(g.code).emit('timer',g.timer);if(g.timer===0)emit(g)}},1000);
const PORT=+process.env.PORT||3000;init().then(()=>server.listen(PORT,'0.0.0.0',()=>console.log('Family Feud V3 on '+PORT))).catch(e=>{console.error(e);process.exit(1)});
