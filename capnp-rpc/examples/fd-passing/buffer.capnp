@0xbf86abe14eff3512;

interface Buffer {
  read @0 () -> (data :Data);
}

interface Exchange {
  get @0 () -> (buffer :Buffer);
  echo @1 (buffer :Buffer) -> (buffer :Buffer);
}
