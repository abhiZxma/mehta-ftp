module.exports = {
    sortByProperty,
    removeDuplicates
};

function sortByProperty( arr, property, descending )
{
  arr.sort( function( a, b )
  {
    var c = a[property].toString()
      , d = b[property].toString()

    if ( c == d ) return 0;
    return Boolean( descending )
      ? d > c ? 1 : -1
      : d < c ? 1 : -1 
  } );
}

function removeDuplicates(array) {
  return array.filter((a, b) => array.indexOf(a) === b)
};