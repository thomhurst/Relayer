// Code colours follow the site palette: ink and slate for structure, cobalt for keywords,
// teal for strings and literals.

/** @type {import('prism-react-renderer').PrismTheme} */
export const relayerLight = {
  plain: {color: '#1b2633', backgroundColor: '#f6f8fb'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#6a7686', fontStyle: 'italic'}},
    {types: ['keyword', 'builtin', 'boolean', 'constant'], style: {color: '#2741c4'}},
    {types: ['class-name', 'function', 'attr-name'], style: {color: '#7a3db8'}},
    {types: ['string', 'char', 'attr-value', 'number', 'inserted'], style: {color: '#0d6e60'}},
    {types: ['operator', 'punctuation'], style: {color: '#4b5768'}},
    {types: ['deleted'], style: {color: '#b3261e'}},
  ],
};

/** @type {import('prism-react-renderer').PrismTheme} */
export const relayerDark = {
  plain: {color: '#dfe5ee', backgroundColor: '#161d28'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#8592a4', fontStyle: 'italic'}},
    {types: ['keyword', 'builtin', 'boolean', 'constant'], style: {color: '#9db0ff'}},
    {types: ['class-name', 'function', 'attr-name'], style: {color: '#d3a9ff'}},
    {types: ['string', 'char', 'attr-value', 'number', 'inserted'], style: {color: '#5fd0bb'}},
    {types: ['operator', 'punctuation'], style: {color: '#a9b4c3'}},
    {types: ['deleted'], style: {color: '#ff8a80'}},
  ],
};
