#!/usr/bin/perl -w
#######################################
###### УДАЛЕНИЕ ТРАНЗИТНОЙ БРОНИ  #####
######## 09.07.22 версия 0.2 ##########
###########################################################################
### Cкрипт ищет рейсы менее часа до отправления и удаляет невыкупленные ###
### «брони транзитных вокзалов» на них (рейсах), а так же эти брони на  ###
### самих транзитных вокзалах (удаленные сервера))  #######################
###########################################################################

use DBI;
$dbh_form = DBI->connect("DBI:Informix:base_local",'XXXXXX','XXXXXX',{AutoCommit=>0,RaiseError=>1,PrintError=>1}) or die "Нет соединения с формирующимся сервером";
$dbh_form->do('set isolation to dirty read');
my $tranzit_end   = $dbh_form->selectrow_array("select value_var from av_config where name_var='tranzit_end'");
my $tranzit_begin = $dbh_form->selectrow_array("select value_var from av_config where name_var='tranzit_begin'");
my $c = $dbh_form->selectrow_array("select current from table(set{1})");
print "=========== $c =============\n";
# Обрабатываем формирующиеся ведомости в состоянии: открыта(1), закрыта(мест нет)(2), опоздание(7), посадка(8)
# в интервале tranzit_begin - tranzit_end до отправления
my $mas_form = $dbh_form->selectall_arrayref("
select
  dved,v.nr,kpp,mmesto,b.nved
from
  av_v v, av_b b, av_r r
where
  dved=date(current)
  and v.totpr >= (current hour to minute + (interval($tranzit_end) hour to minute))
  and v.totpr <= (current hour to minute + (interval($tranzit_begin) hour to minute))
  and dop=0 and sved in (1,2,7,8)
  and v.nved=b.nved and zakaz=1 and tipbr='бронь транзитного АВ' and kpp=kpk
  and v.nday=r.nday and v.nr=r.nr and r.kodp=v.kodp and n_pp=1
order by
  dved,nr,kpp,mmesto");

my $h_form = {};
for (@$mas_form) {
  my ($dved,$nr,$kpp,$mesto,$nved) = @$_;
  # Корректируем переход через сутки
  my $shift_day = shift_day($nved,$kpp);
  if ($shift_day) {$dved = $dbh_form->selectrow_array("select date('$dved')+$shift_day from table(set{1})")}
  # Раскладываем выборку в хэш
  $h_form->{$kpp}{$dved}{$nr}{mesta} .= $mesto.',';
  $h_form->{$kpp}{$dved}{$nr}{nved} = $nved;
  print "$dved,$nr,$kpp,$mesto,$nved\n";
}

$dbh_tran='';
for my $kpp (keys %{$h_form}) {
  # ЦИКЛ ПО ТРАНЗИТНЫМ АВ
  $dbh_form->rollback   if $dbh_form;
  $dbh_tran->rollback   if $dbh_tran;
  $dbh_tran->disconnect if $dbh_tran;
  my $serv_tran = $dbh_form->selectrow_array("select iserv from spserv where kodp=$kpp");
  eval {
    $dbh_tran = DBI->connect("DBI:Informix:base_tran\@$serv_tran",'XXXXXX','XXXXXX',{AutoCommit=>0,RaiseError=>1,PrintError=>1});
  };
  if ( $@ ) {
    print "ERROR: Нет соединения с $serv_tran\n"; 
    next;
  };
  $dbh_tran->do('set isolation to dirty read');
  for my $dved (keys %{$h_form->{$kpp}}) {
    # ЦИКЛ ПО ДАТАМ ОТПРАВЛЕНИЯ
    for my $nr (keys %{$h_form->{$kpp}{$dved}}) {
      # ЦИКЛ ПО РЕЙСАМ
      $h_form->{$kpp}{$dved}{$nr}{mesta} = substr($h_form->{$kpp}{$dved}{$nr}{mesta},0,-1); # Отрезаем последнюю запятую 
      my $nved_form = $h_form->{$kpp}{$dved}{$nr}{nved};
      my $mas_mesta = $dbh_tran->selectall_arrayref("select mesto,v.nved,kolm from av_v v, av_b_tr tr where dved='$dved' and nr=$nr and dop=0 and sved in (1,2,7,8) and tr.nved=v.nved and tr.mmesto not in (select mmesto from av_b where nved=tr.nved)");
      my $nved = $mas_mesta->[0][1];
      if (!$nved) {
        print "!!! Вся бронь закрыта ($serv_tran) !!!\n";
        print "select mesto,v.nved,kolm from av_v v, av_b_tr tr where dved='$dved' and nr=$nr and dop=0 and sved in (1,2,7,8) and tr.nved=v.nved and tr.mmesto not in (select mmesto from av_b where nved=tr.nved)\n";
        next;
      }

      my $mesta;
      for (@$mas_mesta) {$mesta .= @$_[0].','}
      $mesta = substr($mesta,0,-1);
      my $mesta_count = scalar @$mas_mesta;

      # НА ТРАНЗИТНОМ АВ
      eval {
        # УДАЛЯЕМ НЕВЫКУПЛЕННУЮ ТРАНЗИНУЮ БРОНЬ
        $dbh_tran->do("delete from av_b_tr where nved='$nved' and mesto in ($mesta)");
        # УМЕНЬШАЕМ ВМЕСТИМОСТЬ
        $dbh_tran->do("update av_v set kolm=(kolm-$mesta_count),kolmf=(kolmf-$mesta_count) where nved='$nved'");
      };
      if ( $@ ) {
        print "ERROR: Ошибка удаления брони на ТРАНЗИТНОМ АВ ($serv_tran)\n";
        print "select mesto,v.nved,kolm from av_v v, av_b_tr tr where dved='$dved' and nr=$nr and dop=0 and sved in (1,2,7,8) and tr.nved=v.nved and tr.mmesto not in (select mmesto from av_b where nved=tr.nved)\n";
        print "delete from av_b_tr where nved='$nved' and mesto in ($mesta)\n";
        print "update av_v set kolm=(kolm-$mesta_count),kolmf=(kolmf-$mesta_count) where nved='$nved'\n";
        $dbh_tran->rollback;
      } else {
        # НА ФОРМИРУЮЩЕМСЯ АВ
        eval {
          # ПЕРЕНОСИМ БРОНЬ В ВОЗВРАТ
          $dbh_form->do("insert into av_wz select nved,mmesto,tbilet,kpp,1,tipbr,tbtarif,primf,tabnop,extend(current,year to minute),0 from av_z where nved='$nved_form' and tipbr='бронь транзитного АВ' and mmesto in ($mesta)");
          # УДАЛЯЕМ БРОНЬ ИЗ ЛОГА
          $dbh_form->do("delete from av_z where nved='$nved_form' and tipbr='бронь транзитного АВ' and mmesto in ($mesta)");
          # УДАЛЯЕМ БРОНЬ ИЗ ВЕДОМОСТИ
          $dbh_form->do("delete from av_b where nved='$nved_form' and zakaz=1 and tipbr='бронь транзитного АВ' and mmesto in ($mesta)");
        };
        if ($@) {
          print "ERROR: Ошибка удаления брони на ФОРМИРУЮЩЕМСЯ АВ\n";
          print "insert into av_wz select nved,mmesto,tbilet,kpp,1,tipbr,tbtarif,primf,tabnop,extend(current,year to minute),0 from av_z where nved='$nved_form' and tipbr='бронь транзитного АВ' and mmesto in ($mesta)\n";
          print "delete from av_z where nved='$nved_form' and tipbr='бронь транзитного АВ' and mmesto in ($mesta)\n";
          print "delete from av_b where nved='$nved_form' and zakaz=1 and tipbr='бронь транзитного АВ' and mmesto in ($mesta)\n";
          $dbh_form->rollback;
        } else {
          # ФИКСИРУЕМ ТРАНЗАКЦИИ
          $dbh_tran->commit;
          $dbh_form->commit;
        }
      }
    }
  }
}
$c = $dbh_form->selectrow_array("select current from table(set{1})");
$dbh_form->rollback;;
$dbh_form->disconnect;
print "=========== $c =============\n\n";

# Процедура вычисляет смещение в сутках от пункта А (nved) до пункта B (kpp)
sub shift_day {
  my $nved = shift;
  my $kpp  = shift;
  my ($nday,$nr,$kodp) = $dbh_form->selectrow_array("select nday,nr,kodp from av_v where nved='$nved'");
  my $n_pp_otpr = $dbh_form->selectrow_array("select first 1 n_pp from av_r where nday=$nday and nr=$nr and kodp=$kodp");  # n_pp отправления
  my $n_pp_prib = $dbh_form->selectrow_array("select first 1 n_pp from av_r where nday=$nday and nr=$nr and kodp=$kpp");   # n_pp прибытия
  my $shift = 0;
  for ($i=$n_pp_otpr; $i<$n_pp_prib; $i++) {
    # Вычисляем переход на следующие сутки между остановочными пунктами
    $shift += $dbh_form->selectrow_array("select count(*) from av_r r1, av_r r2 where r1.nday=$nday  and r1.nr=$nr and r1.nday=r2.nday and r1.nr=r2.nr and r1.n_pp=$i and r2.n_pp=($i+1) and r1.totpr>r2.tprib");
    # Вычисляем переход на следующие сутки на остановочном пункте
    $shift += $dbh_form->selectrow_array("select count(*) from av_r where nday=$nday and nr=$nr and n_pp=($i+1) and totpr<tprib");
  }
  return $shift;
}
