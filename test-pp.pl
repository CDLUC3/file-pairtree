#!/usr/bin/perl
use File::Pairtree;
$|=1;

my $min_char_int = 0x0000;
my $max_char_int = 0xffff;
#my $max_char_int = 0x00ff;

sub id2codepoints {
    my $id = shift;
    my $cp = "";
    foreach my $c (split(//, $id)) {
        $cp .= sprintf("U+%04x ", ord($c));
    }
    return $cp;
}

sub check {
    my $id = shift;
    my $pp = id2ppath($id);
    my $rt = ppath2id($pp);
    #print sprintf("Checking %s id: '%s', pp: '%s', roundtrip id: '%s'...", id2codepoints($id), $id, $pp, $rt);
    print sprintf("Checking %s, pp: '%s''...", id2codepoints($id), $pp);
    return $id eq $rt;
}

for (my $i = $min_char_int; $i <= $max_char_int; $i++) {
    die "sanity check" if (ord(chr($i)) != $i);
    if (!check(chr($i))) {
        die "did not work!\n";
    } else {
        print "worked.\n";
    }
}

my $max_length = 10000;

while (true) {
    my $length = int(rand($max_length));
    my $id = "";
    for (my $i = 0 ; $i < $length ; $i++) {
        $id .= chr(int(rand($max_char_int)));
    }
    if (!check($id)) {
        die "did not work!\n";
    } else {
        print "worked.\n";
    }
}
