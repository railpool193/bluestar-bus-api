export const state={language:localStorage.getItem('language')||'hu',view:'home',params:{},cleanup:null,status:null}
export function setLanguage(value){state.language=value==='en'?'en':'hu';localStorage.setItem('language',state.language)}
export function disposeView(){if(typeof state.cleanup==='function')state.cleanup();state.cleanup=null}
