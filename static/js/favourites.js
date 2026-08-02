const key='bluestar:favourites';export function favourites(){try{const value=JSON.parse(localStorage.getItem(key)||'[]');return Array.isArray(value)?value:[]}catch{return[]}}
export function isFavourite(type,id){return favourites().some(item=>item.type===type&&item.id===id)}
export function toggleFavourite(item){const values=favourites(),index=values.findIndex(value=>value.type===item.type&&value.id===item.id);if(index>=0)values.splice(index,1);else values.unshift(item);localStorage.setItem(key,JSON.stringify(values));return index<0}
